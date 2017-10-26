const SimpleDB = require('../dynamodb');
const expect = require('chai').expect;

describe('DynamoDB Query Operation', () => {
    describe('Primary Key', () => {
        it('Primary Key', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.query()
                .tableName('MyTable')
                .primaryKey('userId', 'abc')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.KeyConditionExpression).to.be.equal('userId = :userId');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'abc' });
                });
        }).timeout(5000);

        it('Primary Key with Equal', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.query()
                .tableName('MyTable')
                .primaryKey('userId').equal('abc')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.KeyConditionExpression).to.be.equal('userId = :userId');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'abc' });
                });
        }).timeout(5000);
    });    


    describe('Sort Key', () => {
        it('Sort Key (legacy)', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.query()
                .tableName('MyTable')
                .primaryKey('userId').equal('abc')
                .sortKey('createdTime',123,'=')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.KeyConditionExpression).to.be.equal('userId = :userId AND createdTime = :createdTime');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'abc', ':createdTime': 123 });
                });
        }).timeout(5000);

        it('Sort Key', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.query()
                .tableName('MyTable')
                .primaryKey('userId').equal('abc')
                .sortKey('createdTime').equal(123)
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.KeyConditionExpression).to.be.equal('userId = :userId AND createdTime = :createdTime');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'abc', ':createdTime': 123 });
                });
        }).timeout(5000);

        it('Sort query in ascending order', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.query()
                .tableName('MyTable')
                .primaryKey('userId').equal('abc')
                .sortKey().ascendingOrder()
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.KeyConditionExpression).to.be.equal('userId = :userId');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'abc' });
                    expect(params.ScanIndexForward).to.be.equal(true);
                });
        }).timeout(5000);

        it('Sort query in descending order', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.query()
                .tableName('MyTable')
                .primaryKey('userId').equal('abc')
                .sortKey().descendingOrder()
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.KeyConditionExpression).to.be.equal('userId = :userId');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'abc' });
                    expect(params.ScanIndexForward).to.be.equal(false);
                });
        }).timeout(5000);
    });    

    
});