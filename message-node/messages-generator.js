const randomString = require('randomstring');

class MessagesGenerator {
    constructor(guid, options, redis) {
        this._guid = guid;
        this._options = options;
        this._redis = redis;
        // redis key prefix to store messages
        this._prefix = this._options.prefix + ':messages:' + this._guid + ':';
        // counter used to prevent messages redis key collision
        // in case of messages generates faster then 1 message per ms
        // primitive but works well in this context
        this._counter = 0;
    }

    generate(text, callback) {
            text = text || randomString.generate();
            this._redis.set(this._prefix + Date.now() + this._counter, text, (err, result) => {
                if (err) {
                    callback(err);
                    return;
                }

                this._counter++;
                // reset counter if it grown too big
                if (this._counter === Math.MAX_SAFE_INTEGER) {
                    this._counter = 0;
                }
                callback(null, result);
            })
    }
}

exports.MessagesGenerator = MessagesGenerator;