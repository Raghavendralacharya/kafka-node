var createTopic = require('../topics');

var topicsToCreate = [{
    topic: 'testTopic1',
    partitions: 2,
    replicationFactor: 1
},
{
    topic: 'testTopic2',
    partitions: 3,
    replicationFactor: 2
}];
createTopic(topicsToCreate).then((succ) => {
    console.log(succ);
}).catch((err) => {
    console.log(err);
});
