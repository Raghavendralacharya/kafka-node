var kafka = require('kafka-node');
const config = require('./config');
var client = new kafka.KafkaClient(config.kafka_server);

function createTopic(topics){
    return new Promise((resolve, reject)=>{
        client.createTopics(topics, (error, result) => {
            if(error){
                reject(Error);
                return;
            } else{
                resolve('topic created successfully');
                return;
            }
          // result is an array of any errors if a given topic could not be created
        });
    })
}

module.exports = createTopic;