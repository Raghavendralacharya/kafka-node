var kafka = require('kafka-node');
var async = require('async');

function initiateKafkaConsumerGroup(groupName, topicName) {
    var consumerOptions = {
        //host: 'localhost:2181',
        kafkahost:'localhost:9092',
        groupId: groupName,
        autoCommit: true,
        autoCommitIntervalMs: 1000,
        sessionTimeout: 15000,
        fetchMaxBytes: 10 * 1024 * 1024, // 10 MB
        protocol: ['range'],
        fromOffset: 'earliest'
    };

    var consumer = new kafka.ConsumerGroup(Object.assign({ id: 'consumer' }, consumerOptions), topicName);

    consumer.on('message', onMessage)
    consumer.on('error', onError)


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
        async.each([consumer], function (consumer, callback) {
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