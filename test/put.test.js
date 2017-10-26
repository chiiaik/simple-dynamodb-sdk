const SimpleDB = require('../dynamodb');
const expect = require('chai').expect;

describe('DynamoDB Put Operation', () => {
    it('Return All New', () => { 
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.put()
            .tableName('MyTable')
            .item({ name: 'Alice' })
            .attribute('age').equal(20).and().attribute('job').equal('retired')
            .returnValues().allNew()
            .returnItemCollectionMetrics()
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('MyTable');
                expect(params.Item).to.deep.equal({ name: 'Alice' });
                expect(params.ConditionExpression).to.be.equal('age = :age AND job = :job');
                expect(params.ExpressionAttributeValues).to.deep.equal({ ':age': 20, ':job': 'retired' });
                expect(params.ReturnItemCollectionMetrics).to.be.equal('SIZE');
                expect(params.ReturnValues).to.be.equal('ALL_NEW');                
            });
    });

    it('Return All Old', () => { 
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.put()
            .tableName('MyTable')
            .item({ name: 'Alice' })
            .attribute('age').equal(20).and().attribute('job').equal('retired')
            .returnValues().allOld()
            .returnItemCollectionMetrics()
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('MyTable');
                expect(params.Item).to.deep.equal({ name: 'Alice' });
                expect(params.ConditionExpression).to.be.equal('age = :age AND job = :job');
                expect(params.ExpressionAttributeValues).to.deep.equal({ ':age': 20, ':job': 'retired' });
                expect(params.ReturnItemCollectionMetrics).to.be.equal('SIZE');
                expect(params.ReturnValues).to.be.equal('ALL_OLD');                
            });
    });

    it('Return Updated New', () => { 
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.put()
            .tableName('MyTable')
            .item({ name: 'Alice' })
            .attribute('age').equal(20).and().attribute('job').equal('retired')
            .returnValues().updatedNew()
            .returnItemCollectionMetrics()
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('MyTable');
                expect(params.Item).to.deep.equal({ name: 'Alice' });
                expect(params.ConditionExpression).to.be.equal('age = :age AND job = :job');
                expect(params.ExpressionAttributeValues).to.deep.equal({ ':age': 20, ':job': 'retired' });
                expect(params.ReturnItemCollectionMetrics).to.be.equal('SIZE');
                expect(params.ReturnValues).to.be.equal('UPDATED_NEW');                
            });
    });

    it('Return Updated Old', () => { 
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.put()
            .tableName('MyTable')
            .item({ name: 'Alice' })
            .attribute('age').equal(20).and().attribute('job').equal('retired')
            .returnValues().updatedOld()
            .returnItemCollectionMetrics()
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('MyTable');
                expect(params.Item).to.deep.equal({ name: 'Alice' });
                expect(params.ConditionExpression).to.be.equal('age = :age AND job = :job');
                expect(params.ExpressionAttributeValues).to.deep.equal({ ':age': 20, ':job': 'retired' });
                expect(params.ReturnItemCollectionMetrics).to.be.equal('SIZE');
                expect(params.ReturnValues).to.be.equal('UPDATED_OLD');                
            });
    });
});    