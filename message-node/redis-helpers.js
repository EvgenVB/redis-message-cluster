exports.scan = function scan(redis, cursor, matchKey, count) {
    return new Promise((resolve, reject) => {
        redis.scan(cursor, 'MATCH', matchKey, 'COUNT', count, (err, result) => {
            if (err) {
                return reject(err);
            }
            resolve(result);
        })
    });
}

exports.getAndDelete = function getAndDelete(redis, key) {
    return new Promise((resolve, reject) => {
        redis.multi()
                .get(key)
                .del(key)
                .exec((err, replies) => {
                    if (err) {
                        reject(err);
                        return;
                    }

                    if (replies && replies.length > 0) {
                        resolve(replies[0]);
                    } else {
                        resolve();
                    }
                });
    });
}

exports.writeError = function writeError(redis, prefix, err, message) {
    return new Promise((resolve, reject) => {
        const errorMessageObject = {
            stamp: Date.now(),
            msg: err.message,
            stack: err.stack,
            message
        }

        redis.set(prefix + Date.now(), JSON.stringify(errorMessageObject), (err, result) => {
            if (err) {
                reject(err);
            }
            resolve(result);
        })
    });
}

exports.readAndDeleteError = function readAndDeleteError(redis, key) {
    return new Promise((resolve, reject) => {
        exports.getAndDelete(redis, key)
                .then((message) => {
                    try {
                        const errorObject = JSON.parse(message);
                        resolve(errorObject);
                    } catch (err) {
                        reject(err);
                    }
                })
                .catch(reject);
    });
}