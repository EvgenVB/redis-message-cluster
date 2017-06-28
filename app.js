let displayErrorsMode = false;
let testLimit = false;
let prefix = 'messages-cluster-app';

// TODO: argv npm package instead of this shame
process.argv.forEach((val, index) => {
    switch (val) {
        case 'getErrors':
            displayErrorsMode = true;
            break;
        case 'testLimit':
            testLimit = true;
            break;
        default:
            break;
    }

    if (val.indexOf('redisPrefix') === 0) {
        const splittedValue = val.split('=');
        if (splittedValue.length > 1
                && splittedValue[1]
                && splittedValue[1].trim
                && splittedValue[1].trim().length > 0) {
            prefix = splittedValue[1];
        }
    }
});

const redis = require('redis').createClient(6379, 'localhost');
const MessageNode = require('./message-node').MessageNode;
const messageNode = new MessageNode({
    // redlock pkg settings
    redlock: {
        retryCount: 0
    },
    // app redis keys prefix
    prefix: prefix,
    // lock for generation-mode node function
    generatorLockResource: prefix + ':generator:lock',
    // TTL of generator-mode node lock
    generatorLockTTL: 500,
    // interval to check generation-mode lock
    testGeneratorLockInterval: 400,
    // interval to update TTL of generation-mode lock
    generatorLockExtendInterval: 300,
    // scanning messages to handle interval
    handleMessagesInterval: 500,
    // generate message interval
    generateMessagesInterval: 500,
    // count of keys get from redis to scan by per one scan request
    scanMessagesBatchCount: 1000
}, redis, displayErrorsMode, testLimit);

messageNode.start().then(() => {
    if (displayErrorsMode) {
        process.exit();
    }
    console.log('Started');
});