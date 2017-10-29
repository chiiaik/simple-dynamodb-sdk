const SimpleDB = require('../dynamodb');
const expect = require('chai').expect;

describe('DynamoDB Batch Get Operation', () => {
    it('Basic', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.batchGet()
            .tableName('MyTable1')
            .primaryKey('userId').equal('aaa').sortKey('timestamp').equal(111)
            .primaryKey('userId').equal('bbb').sortKey('timestamp').equal(222)
            .projectionExpression('a,b,c')
            .tableName('MyTable2')
            .primaryKey('userId').equal('ccc').sortKey('timestamp').equal(333)
            .primaryKey('userId').equal('ddd').sortKey('timestamp').equal(444)
            .projectionExpression('d,e,f')
            .isConsistentRead()
            .returnConsumedCapacity('TOTAL')
            .dryRun()
            .then(params => {
                console.log(JSON.stringify(params, null, 2));
                // expect(params.TableName).to.be.equal('MyTable');
                // expect(params.Key).to.deep.equal({ userId: 123, timestamp: 456 });
            });
    });
});