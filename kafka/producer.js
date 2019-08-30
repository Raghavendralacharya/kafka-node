const kafka = require('kafka-node');
const config = require('./config');

try {
    function sendMessageToTopic(payload, payload1) {
        return new Promise((resolve, reject) => {
            const client = new kafka.KafkaClient(config.kafka_server);
            const producer = new kafka.HighLevelProducer(client, { requireAcks: 1 });
            producer.on('ready', async function () {
                producer.send(payload, (err, data) => {
                    if (err) {
                        console.log('[kafka-producer -> ]: broker update failed', err);
                        reject(err);
                    } else {
                        console.log('[kafka-producer -> ]: broker update success');
                        resolve('[kafka-producer -> ]: broker update success');
                    }
                });
            });
            producer.on('error', function (err) {
                console.log(err);
                console.log('[kafka-producer -> ' + kafka_topic + ']: connection errored');
                throw err;
            });
        })
    }
}
catch (e) {
    console.log(e);
}

module.exports = sendMessageToTopic;