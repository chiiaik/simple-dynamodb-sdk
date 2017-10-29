const SimpleDB = require('../dynamodb');
const expect = require('chai').expect;

describe('DynamoDB Delete Operation', () => {
    it('Basic', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.get()
            .tableName('MyTable')
            .primaryKey('userId').equal(123)
            .sortKey('timestamp').equal(456)
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('MyTable');
                expect(params.Key).to.deep.equal({ userId: 123, timestamp: 456 });
            });
    });
});