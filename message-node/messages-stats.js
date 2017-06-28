class MessagesStats {
    constructor(guid, prefix, redis) {
        this._prefix = prefix + ':stats:' + guid;
        this._matchPrefix = prefix + ':stats:*';
        this._redis = redis;
    }

    incrGenerated(by) {
        return this._increment(this._prefix + ':generated', by);
    }

    incrStats(statsObject) {
        const promises = [];
        if (statsObject.handled > 0) {
            console.log(statsObject.handled);
            promises.push(this.incrHandled(statsObject.handled));
        }

        if (statsObject.handleErrors > 0) {
            promises.push(this.incrHandleErrors(statsObject.handleErrors));
        }

        if (statsObject.scanned > 0) {
            console.log(statsObject.scanned);
            promises.push(this.incrScanned(statsObject.scanned));
        }

        return Promise.all(promises);
    }

    incrHandled(by) {
        return this._increment(this._prefix + ':handled', by);
    }

    incrScanned(by) {
        return this._increment(this._prefix + ':scanned', by);
    }

    incrHandleErrors(by) {
        return this._increment(this._prefix + ':handle_errors', by);
    }

    getStats() {
        const statResult = {
            nodesStat: {},
            generated: 0,
            handled: 0,
            scanned: 0,
            handle_errors: 0
        }

        return new Promise((resolve, reject) => {
            let counter;

            this._redis.keys(this._matchPrefix, (err, keys) => {
                if (err) {
                    reject(err);
                    return;
                }

                counter = keys.length;
                keys.forEach(key => {
                    try {
                        const keysParts = key.split(':');
                        const nodeGUID = keysParts[keysParts.length - 2];
                        const statsField = keysParts[keysParts.length - 1];

                        if (statResult.hasOwnProperty(statsField)) {
                            this._redis.get(key, (err, result) => {
                                if (err) {
                                    console.error(err);
                                    throw err;
                                }

                                let value = 0;
                                if (result && !isNaN(parseInt(result))) {
                                    value = parseInt(result, 10);
                                    statResult[statsField] += value;

                                    if (!statResult.nodesStat.hasOwnProperty(nodeGUID)) {
                                        statResult.nodesStat[nodeGUID] = {
                                            generated: 0,
                                            handled: 0,
                                            scanned: 0,
                                            handle_errors: 0
                                        }
                                    }
                                    statResult.nodesStat[nodeGUID][statsField] += value;
                                }
                                checkDone();
                            });
                        } else {
                            checkDone();
                        }
                    } catch (e) {
                        console.error(e);
                        reject(e);
                        return true; // exit forEach loop
                    }
                });
            });

            function checkDone() {
                counter--;
                if (counter === 0) {
                    resolve(statResult);
                }
            }
        });
    }

    _increment(key, value) {
        return new Promise((resolve, reject) => {
            this._redis.INCRBY(key, value, (err) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve();
            })
        });
    }
}

exports.MessagesStats = MessagesStats;