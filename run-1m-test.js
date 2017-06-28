const redis = require('redis');
const { exec } = require('child_process');
const MessagesStats = require('./message-node/messages-stats').MessagesStats;
const redisClient = redis.createClient(6379, 'localhost');
const procs = [];
const prefix = '1m-test-run';
const messagesStats = new MessagesStats('', prefix, redisClient);

function cleanRedisKeys(client, cb) {
    let defCounter = 0;
    client.keys(prefix + ':*', (err, keys) => {
        defCounter = keys.length;
        if (defCounter === 0) {
            cb();
        }
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

cleanRedisKeys(redisClient, () => {
    for (let i = 0; i < 3; i++) {
        procs.push(exec(`NODE_ENV=test node ${__dirname}/app.js testLimit redisPrefix=${prefix}`));
    }

    setInterval(()=>{
        messagesStats.getStats().then((stats) => {
            const nodes = Object.keys(stats.nodesStat);
            let overallMissScan = 0;
            nodes.forEach(node => {
                const value = (stats.nodesStat[node].handled + stats.nodesStat[node].handle_errors)  / stats.nodesStat[node].scanned;
                overallMissScan += isNaN(value) ? 0 : value;
            });
            console.log(`Generated: ${stats.generated}, Handled: ${stats.handled}, Scanned: ${stats.scanned}, HandleErrors: ${stats.handle_errors}, AVG ScanMiss: ${(100 - (overallMissScan * 100 / (nodes.length - 1))).toFixed(2)}%`);
            if (stats.generated === 1000000) {
                console.log(`Generation finished`);
            }

            if (stats.handled + stats.handle_errors  === 1000000) {
                console.log(`Handling finished`);
                process.nextTick(()=> {
                    process.exit();
                });
            }
        })
    }, 5000);
});

