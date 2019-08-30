var sendMessage = require('../producer')

try {
    payloads = [
        { topic: 'testTopic1', messages: ['hi', 'hello', 'world'] },
        { topic: 'testTopic1', messages: ['hi1', 'hello1', 'world1'] },
        { topic: 'testTopic1', messages: ['hi2', 'hello2', 'world2'] }
    ];
    sendMessage(payloads).then((succ) => {
        console.log("success", succ);
    }).catch((err) => {
        console.log("failed", err);
    })
}
catch (e) {
    console.log(e);
}