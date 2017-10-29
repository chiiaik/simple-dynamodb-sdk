const SimpleDB = require('../dynamodb');
const expect = require('chai').expect;

describe('DynamoDB Batch Write Operation', () => {
    it('Basic', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.batchWrite()
            .tableName('MyTable1')
            .delete().primaryKey('userId').equal('aaa')
            .delete().primaryKey('userId').equal('bbb')
            .put().primaryKey('userId').equal('ccc')
            .attribute('firstName').equal('John')
            .attribute('lasName').equal('Doe')
            .put().primaryKey('userId').equal('ddd')
            .attribute('firstName').equal('Alice')
            .attribute('lasName').equal('Tan')
            .tableName('MyTable2')
            .delete().primaryKey('userId').equal('eee')
            .delete().primaryKey('userId').equal('fff')
            .put().primaryKey('userId').equal('ggg')
            .attribute('firstName').equal('Bob')
            .attribute('lasName').equal('Lim')
            .put().primaryKey('userId').equal('hhh')
            .attribute('firstName').equal('Charlie')
            .attribute('lasName').equal('Tan')
            .returnConsumedCapacity('TOTAL')
            .returnItemCollectionMetricsSize()
            .dryRun()
            .then(params => {
                console.log(JSON.stringify(params, null, 2));
                // expect(params.TableName).to.be.equal('MyTable');
                // expect(params.Key).to.deep.equal({ userId: 123, timestamp: 456 });
            });
    });

    it('Array', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.batchWrite()
            .tableName('MyTable1')
            .delete([{ userId: 'aaa' }, { userId: 'bbb' }])
            .put([{ userId: 'ccc', name: 'Alice' }, { userId: 'ddd', name: 'Bob' }])
            .returnConsumedCapacity('TOTAL')
            .returnItemCollectionMetricsSize()
            .dryRun()
            .then(params => {
                console.log(JSON.stringify(params, null, 2));
                // expect(params.TableName).to.be.equal('MyTable');
                // expect(params.Key).to.deep.equal({ userId: 123, timestamp: 456 });
            });
    });
});