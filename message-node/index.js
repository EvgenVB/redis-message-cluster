const uuidV4 = require('node-uuid').v4;
const Redlock = require('redlock');
const MessageHandler = require('./messages-handler').MessageHandler;
const MessagesGenerator = require('./messages-generator').MessagesGenerator;
const MessagesStats = require('./messages-stats').MessagesStats;
const redisHelpers = require('./redis-helpers');

const STATES = {
    NOT_RUN: 0,
    DISPLAY_ERRORS: 1,
    HANDLER: 2,
    GENERATOR: 3
};

class MessageNode {

    constructor(options, redis, displayErrorsMode = false, testLimit = false) {
        this._options = options;
        this._redis = redis;
        this._state = displayErrorsMode ? STATES.DISPLAY_ERRORS : STATES.NOT_RUN;
        this._guid = uuidV4();
        this._handler = new MessageHandler(this._guid, this._options, this._redis);
        this._generator = new MessagesGenerator(this._guid, this._options, this._redis);
        this._stats = new MessagesStats(this._guid, this._options.prefix, this._redis);
        this._limit = 1000000;
        this._isLimit = testLimit;
    }

    get guid() {
        return this._guid;
    }

    get isRunning() {
        return this._state > STATES.NOT_RUN;
    }

    get isGenerator() {
        return this._state === STATES.GENERATOR;
    }

    async start() {
        // If run with getErrors cmd arg, just display and delete errors
        if (this._state === STATES.DISPLAY_ERRORS) {
            await this._displayErrors();
            return;
        }

        // use redlock module in case of not to develop #9001 version of redlock algorythm
        if (!this._redlock) {
            this._redlock = new Redlock([this._redis], this._options.redlock);
        }
        this._testGeneratorLock();
    }

    stop() {
        if (this.isGenerator) {
            this._redlock.release(this._lock);
        }

        this._state = STATES.NOT_RUN;

        this._clearGeneratorLock();
        this._clearMessagesHandling();
        this._clearGeneratorExtending();
    }

    _clearGeneratorLock() {
        if (this._testGeneratorLockTimeout) {
            clearTimeout(this._testGeneratorLockTimeout);
        }
    }

    _clearMessagesHandling() {
        if (this._handleMessagesTimeout) {
            clearTimeout(this._handleMessagesTimeout);
        }
    }

    _clearGeneratorExtending() {
        if (this._extendGeneratorLockTimeout) {
            clearTimeout(this._extendGeneratorLockTimeout);
        }
    }

    _clearTestGenerator() {
        if (this._testGeneratorLockTimeout) {
            clearTimeout(this._testGeneratorLockTimeout);
        }
    }

    // brings node in a generator mode, runs extend-lock and generate-message loops
    _setupGenerator(lock) {
        this._state = STATES.GENERATOR;
        this._lock = lock;
        // cleanup "handler" mode loops if exists
        this._clearTestGenerator();
        this._clearMessagesHandling();

        this._extendGeneratorLock(lock);
        this._generateMessagesLoop();
    }

    // brings node in a handler mode,
    // runs one-day-i-will-be-a-generator and i-handle-messages-like-a-pro loops
    _setupHandler() {
        this._state = STATES.HANDLER;
        // cleanup extension of generator lock loop
        this._clearGeneratorExtending();

        this._handleMessages();
    }

    // checks generator lock, tries to capture it, can be successful if last generator node stuck
    // and has not extend lock in time, or its process had crashed TTL mills ago
    _testGeneratorLock() {
        if (this.isGenerator) {
            return;
        }
        this._testGeneratorLockTimeout = setTimeout(() => {
            this._redlock.lock(this._options.generatorLockResource, this._options.generatorLockTTL)
                    .then((lock) => {
                        // OMG! I-AM-A-GENERATOR-NOW! Lets start extend-lock loop
                        this._setupGenerator(lock);
                    })
                    .catch((err) => {
                        if (!this.isRunning) {
                            this._setupHandler();
                        }
                        this._testGeneratorLock();
                    });

        }, this._options.testGeneratorLockInterval);
    }

    // Extends generator lock TTL, brings current node to handler mode on fail
    _extendGeneratorLock(lock) {
        if (!this.isRunning || !this.isGenerator) {
            return;
        }
        this._extendGeneratorLockTimeout = setTimeout(() => {
            lock.extend(this._options.generatorLockTTL, (err, lock) => {
                if (err) {
                    this._setupHandler(); // Oh no! We are not a generator anymore.
                    return;
                }

                this._extendGeneratorLock(lock);
            })
        }, this._options.generatorLockExtendInterval);
    }

    // Handler mode loop to handle messages
    _handleMessages() {
        if (!this.isRunning || this.isGenerator) {
            return;
        }
        this._handleMessagesTimeout = setTimeout(() => {
            this._handler.handleMessages().then((stats) => {
                return this._stats.incrStats(stats);
            }).then(() => {
                this._handleMessages();
            });
        }, this._options.handleMessagesInterval);
    }

    // Generator mode loop to generate messages
    _generateMessagesLoop() {
        if (!this.isRunning
                || !this.isGenerator
                || (this._isLimit && this._limit === 0)) {
            return;
        }

        // special test 1m case
        if (this._isLimit) {
            this._limit--;
        }

        if (process.env.NODE_ENV === 'test') {
            this._generateMessage();
        } else {
            this._handleMessagesTimeout = setTimeout(() => {
                this._generateMessage();
            }, this._options.generateMessagesInterval);
        }
    }

    _generateMessage() {
        this._generator.generate(false, async (err, result) => {
            await this._stats.incrGenerated(1);
            this._generateMessagesLoop();
        });
    }

    // Gets all handle-messages-errors from redis and prints it to console
    async _displayErrors() {
        let cursor;
        while (cursor !== '0') {
            cursor = cursor || '0';
            const result = await redisHelpers.scan(this._redis, cursor, this._handler.errorsPrefix + '*', '50');
            if (result && result.length > 1) {
                cursor = result[0];
                const keys = result[1];
                for (let i = 0; i < keys.length; i++) {
                    const key = keys[i];
                    try {
                        const errorObject = await redisHelpers.readAndDeleteError(this._redis, key);
                        console.log(`[${new Date(errorObject.stamp)}]`,
                                'message:', errorObject.message,
                                '\nstack:', errorObject.stack);
                    } catch (err) {
                        console.error(err);
                    }
                }
            }
        }
    }
}

exports.MessageNode = MessageNode;
exports.STATES = STATES;


