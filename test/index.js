process.env.NODE_ENV = 'test';
const chai = require('chai');
const expect = chai.expect;
const redis = require('redis');
const uuidV4 = require('node-uuid').v4;
const prefix = 'test:' + uuidV4();
const client = redis.createClient(6379, 'localhost');
const MessagesNode = require('../message-node').MessageNode;

describe('Message generator', () => {
    const MessagesGenerator = require('../message-node/messages-generator').MessagesGenerator;
    const options = {
        prefix: prefix
    };
    const guid = uuidV4();

    const messageGenerator = new MessagesGenerator(guid, options, client);

    after(function (done) {
        cleanRedisKeys(client, done);
    });

    it('expect guid was defined', () => {
        expect(messageGenerator).to.have.property('_guid');
        expect(messageGenerator._guid).to.equal(guid);
    });

    it('expect prefix defined', () => {
        expect(messageGenerator).to.have.property('_prefix');
        expect(messageGenerator._prefix).to.equal(options.prefix + ':messages:' + messageGenerator._guid + ':');
    });

    it('expect message generation procedure will be done without any problems', (done) => {
        messageGenerator.generate('test_string').then(() => {
            client.keys(messageGenerator._prefix + '*', (err, result) => {
                expect(err).to.be.null;
                expect(result).to.not.be.empty;

                client.get(result[0], (err, result) => {
                    expect(err).to.be.null;
                    expect(result).to.equal('test_string');
                    done();
                });
            })
        }).catch(e => {
            throw e;
        });
        ;
    });
});

describe('Message handler', () => {
    const MessagesHandler = require('../message-node/messages-handler').MessageHandler;
    const options = {
        prefix: prefix,
        scanMessagesBatchCount: 10
    };
    const guid = uuidV4();
    const messagesHandler = new MessagesHandler(guid, options, client);
    const PREGENERATE_MESSAGES_COUNT = 10000;

    before(function (done) {
        generateMessages(PREGENERATE_MESSAGES_COUNT, guid, options, client).then(() => {
            done();
        });
    });

    after(function (done) {
        cleanRedisKeys(client, done);
    });

    it('expect guid was defined', () => {
        expect(messagesHandler).to.have.property('_guid');
        expect(messagesHandler._guid).to.equal(guid);
    });

    it('expect matchKey was defined', () => {
        expect(messagesHandler).to.have.property('_matchKey');
        expect(messagesHandler._matchKey).to.equal(options.prefix + ':messages:*');
    });

    it('expect errorsPrefix was defined', () => {
        expect(messagesHandler).to.have.property('errorsPrefix');
        expect(messagesHandler.errorsPrefix).to.equal(options.prefix + ':handling_errors:');
    });

    it('expect redis has valid pregenerated messages count', (done) => {
        client.keys(options.prefix + ':*', (err, keys) => {
            expect(err).to.be.null;
            expect(keys).to.have.lengthOf(PREGENERATE_MESSAGES_COUNT);
            done();
        });
    });

    it('expect all messages handled and deleted by MessagesHandler#handleMessages function call', (done) => {
        messagesHandler.handleMessages()
                .then(() => {
                    client.keys(options.prefix + ':messages:*', (err, keys) => {
                        expect(err).to.be.null;
                        expect(keys).to.have.lengthOf(0);
                        done();
                    });
                })
                .catch(e => {
                    throw e;
                });
    })

    it('expect that 5% of handled messages has been handled with error', (done) => {
        client.keys(options.prefix + ':handling_errors:*', (err, keys) => {
            expect(err).to.be.null;
            expect(keys).to.have.lengthOf.at.within(PREGENERATE_MESSAGES_COUNT * 0.04, PREGENERATE_MESSAGES_COUNT * 0.06);
            done();
        });
    });
});

describe('Messages node', () => {
    const options = {
        redlock: {
            retryCount: 0
        },
        prefix: prefix,
        generatorLockResource: prefix + ':generator:lock',
        generatorLockTTL: 500,
        testGeneratorLockInterval: 400,
        generatorLockExtendInterval: 300,
        handleMessagesInterval: 500,
        generateMessagesInterval: 0,
        scanMessagesBatchCount: 10
    };

    const guid = uuidV4();
    const messagesNodes = [new MessagesNode(options, client),
        new MessagesNode(options, client),
        new MessagesNode(options, client)];

    after(function (done) {
        messagesNodes.forEach(node => node.stop());
        setTimeout(() => {
            cleanRedisKeys(client, done);
        }, options.generateMessagesInterval);
    });

    it('expect options was defined', () => {
        expect(messagesNodes[0]._options).to.deep.equal(options);
    })

    it('expect node is not running', () => {
        expect(messagesNodes[0].isRunning).to.be.false;
    })

    it('expect node is not in a generator-mode', () => {
        expect(messagesNodes[0].isGenerator).to.be.false;
    })

    it('expect node started, became an generator and generated some messages', (done) => {
        messagesNodes[0].start().then(() => {
            setTimeout(() => {
                expect(messagesNodes[0].isRunning).to.be.true;
                expect(messagesNodes[0].isGenerator).to.be.true;
                setTimeout(() => {
                    client.keys(options.prefix + ':messages:*', (err, keys) => {
                        expect(err).to.be.null;
                        expect(keys).to.have.lengthOf.at.least(1);
                        done();
                    });
                }, options.generateMessagesInterval * 1.5 + 100);
            }, options.testGeneratorLockInterval * 1.5);
        });
    });

    let generated;

    it('expect node stops', (done) => {
        messagesNodes[0].stop();
        expect(messagesNodes[0].isRunning).to.be.false;
        setTimeout(() => {
            generated = messagesNodes[0]._generator._counter;
            setTimeout(() => {
                expect(messagesNodes[0]._generator._counter).to.be.equal(generated);
                done();
            }, options.generateMessagesInterval);
        }, options.generateMessagesInterval);
    });

    it('expect node starts again, became an generator and generated some messages', (done) => {
        messagesNodes[0].start().then(() => {
            setTimeout(() => {
                expect(messagesNodes[0].isRunning).to.be.true;
                expect(messagesNodes[0].isGenerator).to.be.true;
                setTimeout(() => {
                    expect(messagesNodes[0]._generator._counter).to.be.above(generated);
                    done();
                }, options.generateMessagesInterval * 1.5 + 100);
            }, options.testGeneratorLockInterval * 1.5);
        });
    });

    it('expect another node instances starts in handler-mode', (done) => {
        messagesNodes[1].start().then(() => {
            setTimeout(() => {
                expect(messagesNodes[1].isRunning).to.be.true;
                expect(messagesNodes[1].isGenerator).to.be.false;
                messagesNodes[2].start().then(() => {
                    setTimeout(() => {
                        expect(messagesNodes[2].isRunning).to.be.true;
                        expect(messagesNodes[2].isGenerator).to.be.false;
                        done();
                    }, options.testGeneratorLockInterval * 1.5);
                });
            }, options.testGeneratorLockInterval * 1.5);
        });
    });

    it('expect one node will switch into generator-mode after current generator-mode node stops', (done)=>{
        messagesNodes[0].stop();
        setTimeout(() => {
            expect(true).to.be.oneOf([messagesNodes[1].isGenerator, messagesNodes[2].isGenerator]);
            expect(messagesNodes[1].isGenerator).not.to.be.equal(messagesNodes[2].isGenerator);
            done();
        }, options.testGeneratorLockInterval * 1.5);
    });

    it('expect previously stopped node will starts in handler-mode', (done)=>{
        messagesNodes[0].start().then(() => {
            setTimeout(() => {
                expect(messagesNodes[0].isRunning).to.be.true;
                expect(messagesNodes[0].isGenerator).to.be.false;
                done();
            }, options.testGeneratorLockInterval * 1.5);
        });
    });
});

async function generateMessages(count, guid, options, client) {
    const MessagesGenerator = require('../message-node/messages-generator').MessagesGenerator;
    const messageGenerator = new MessagesGenerator(guid, options, client);
    for (let i = 0; i < count; i++) {
        await messageGenerator.generate();
    }
}

function cleanRedisKeys(client, cb) {
    let defCounter = 0;
    client.keys(prefix + ':*', (err, keys) => {
        defCounter = keys.length;
        keys.forEach(key => {
            client.del(key, (err) => {
                checkDefDone();
            })
        })
    });

    function checkDefDone() {
        defCounter--;
        if (defCounter === 0) {
            cb();
        }
    }
}