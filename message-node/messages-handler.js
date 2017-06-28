const redisHelpers = require('./redis-helpers');

class MessageHandler {
    constructor(guid, options, redis) {
        this._guid = guid;
        this._options = options;
        this._redis = redis;
        // redis key pattern to find stored messages
        this._matchKey = this._options.prefix + ':messages:*';
        // redis key prefix to store message handle errors
        this.errorsPrefix = this._options.prefix + ':handling_errors:';
        // count of message keys get from redis by one scan request
        this._options.scanBatchCount = this._options.scanMessagesBatchCount || '10';
    }

    async handleMessages(cursor = '0') {
        const stats = {
            scanned: 0,
            handled: 0,
            handleErrors: 0
        }

        try {
            const scanResult = await redisHelpers.scan(this._redis, cursor, this._matchKey, this._options.scanMessagesBatchCount);
            if (scanResult && scanResult.length && scanResult.length > 1) {
                cursor = scanResult[0];
                const messagesKeys = scanResult[1];
                if (messagesKeys && messagesKeys.length) {
                    stats.scanned = messagesKeys.length;
                    for (let i = 0; i < messagesKeys.length; i++) {
                        const messageKey = messagesKeys[i];
                        try {
                            const message = await redisHelpers.getAndDelete(this._redis, messageKey);
                            if (message) {
                                try {
                                    this._handleMessage(message);
                                    stats.handled++;
                                } catch (err) {
                                    // Save message handling error to redis
                                    await redisHelpers.writeError(this._redis, this.errorsPrefix, err, message);
                                    stats.handleErrors++;
                                }
                            }
                        } catch (err) {
                            continue;
                        }
                    }
                }
            }
        } catch (err) {
            console.error(err);
            return;
        }

        // No messages was found in redis at this moment
        if (cursor === '0') {
            return stats;
        }

        // recursive call
        const stat = await this.handleMessages(cursor);
        stats.handled += stat.handled;
        stats.scanned += stat.scanned;
        stats.handleErrors += stat.handleErrors;

        return stats;
    }

    // handling stub to generate handling error
    _handleMessage(message) {
        if (Math.random() < 0.05) {
            throw new Error('5% to throw error chance');
        }
    }
}

exports.MessageHandler = MessageHandler;