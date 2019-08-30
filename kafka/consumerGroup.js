var kafka = require('kafka-node');
var async = require('async');

function initiateKafkaConsumerGroup(groupName, topicName) {
    var consumerOptions = {
        host: 'localhost:2181',
        groupId: groupName,
        autoCommit: true,
        autoCommitIntervalMs: 1000,
        sessionTimeout: 15000,
        fetchMaxBytes: 10 * 1024 * 1024, // 10 MB
        protocol: ['roundrobin'],
        fromOffset: 'earliest',
    };

    var consumerG1C1 = new kafka.ConsumerGroup(Object.assign({ id: 'consumer1' }, consumerOptions), topicName);
    var consumerG1C2 = new kafka.ConsumerGroup(Object.assign({ id: 'consumer2' }, consumerOptions), topicName);
    var consumerG1C3 = new kafka.ConsumerGroup(Object.assign({ id: 'consumer3' }, consumerOptions), topicName);

    consumerG1C1.on('message', onMessage)
    consumerG1C1.on('error', onError)
    consumerG1C2.on('message', onMessage);
    consumerG1C2.on('error', onError)
    consumerG1C3.on('message', onMessage)
    consumerG1C3.on('error', onError)


    function onError(error) {
        console.error(error);
        console.error(error.stack);
    }
    function onMessage(message) {
        console.log(
            '%s read msg=%s Topic="%s" Partition=%s Offset=%d',
            this.client.clientId,
            message.value,
            message.topic,
            message.partition,
            message.offset
        );
    }

    process.once('SIGINT', function () {
        async.each([consumerG1C1,consumerG1C2,consumerG1C3], function (consumer, callback) {
            consumer.close(true, function(error) {
                if (error) {
                    console.log("Consuming closed with error", error);
                } else {
                    console.log("Consuming closed");
                }
            });
        });
    });

    console.log('Started Consumer for topic "' + topicName + '" in group "' + groupName + '"');
};
module.exports = initiateKafkaConsumerGroup