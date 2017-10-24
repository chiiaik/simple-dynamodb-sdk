const SimpleDB = require('./dynamodb');

const config = {
    accessKeyId: 'AKIAJIOTBQKBPZUOHPYQ',
    secretAccessKey: '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',
    region: 'ap-southeast-1'
}

function testQuery() {
    let db = new SimpleDB(config);

    return db.query()
        .tableName('Tinker-NearNet-dev.Chat.Message')
        .primaryKey('senderId', 'de5bcd05-6387-4d08-baf6-00b981e93c62')
        .attributeValueEqualTo('content', 'two')
        .and()
        .attributeValueIn('messageId', ['e91c2258-1dbc-43fa-80c3-cf42b583915c','1d313f35-439c-488c-aa44-0f0f6df67e62'])                      
        .run()
        .then(result => {
            console.log(result);
        });  
}

function testGet() {
    let db = new SimpleDB(config);

    return db.get()
        .tableName('Tinker-NearNet-dev.Chat.Message')
        .primaryKey('senderId', 'de5bcd05-6387-4d08-baf6-00b981e93c62')
        .sortKey('createdTime',1505969093000)
        .run()
        .then(result => {
            console.log(result);
        });  
}

function testScan() {
    let db = new SimpleDB(config);

    return db.scan()
        .tableName('Tinker-NearNet-dev.Chat.Message')
        .run()
        .then(result => {
            console.log(result);
        });  
}

testScan();
