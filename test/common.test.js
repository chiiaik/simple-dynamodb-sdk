const SimpleDB = require('../dynamodb');
const expect = require('chai').expect;
const assert = require('chai').assert;


describe('DynamodDB Common Operation', () => {
    describe('Attribute Comparison', () => { 
        it('Attribute Equal', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').equal('john')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('userId = :userId');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'john' });
                });  
        }).timeout(5000);
    
        it('Attribute NotEqual', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').notEqual('john')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('userId <> :userId');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'john' });
                });  
        }).timeout(5000);
    
        it('Attribute LessThan', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').lessThan('john')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('userId < :userId');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'john' });
                });  
        }).timeout(5000);
    
        it('Attribute MoreThan', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').greaterThan('john')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('userId > :userId');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'john' });
                });  
        }).timeout(5000);
    
        it('Attribute LessThanOrEqual', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').lessThanOrEqual('john')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('userId <= :userId');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'john' });
                });  
        }).timeout(5000);
    
        it('Attribute MoreThanOrEqual', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').greaterThanOrEqual('john')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('userId >= :userId');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'john' });
                });  
        }).timeout(5000);
    });
    
    describe('Attribute Range', () => {
        it('Attribute Between', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').between('a', 'b')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('userId BETWEEN :userIdLower AND :userIdUpper');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userIdLower': 'a', ':userIdUpper': 'b' });
                });
        }).timeout(5000);

        it('Attribute In', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').in(['a', 'b', 'c'])
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('userId IN (:userId0,:userId1,:userId2)');
                    expect(params.ExpressionAttributeValues).to.deep.equal({
                        ':userId0': 'a',
                        ':userId1': 'b',
                        ':userId2': 'c'
                    });
                });
        }).timeout(5000);
    });    

    describe('Attribute Functions', () => {
        it('Attribute Exists', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').exists()
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('attribute_exists (userId)');
                });
        }).timeout(5000);

        it('Attribute Not Exists', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').notExists()
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('attribute_not_exists (userId)');
                });
        }).timeout(5000);

        it('Attribute Begins With', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').beginsWith('abc')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('begins_with (userId, :userId)');
                    expect(params.ExpressionAttributeValues).to.deep.equal({
                        ':userId': 'abc'
                    });
                });
        }).timeout(5000);

        it('Attribute Contains', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').contains('abc')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('contains (userId, :userId)');
                    expect(params.ExpressionAttributeValues).to.deep.equal({
                        ':userId': 'abc'
                    });
                });
        }).timeout(5000);

        describe('Attribute Size', () => {
            it('Size Equal', () => {
                let db = new SimpleDB({ dummy: 'dummy' });
                return db.scan()
                    .tableName('MyTable')
                    .attribute('userId').size().equal(20)
                    .dryRun()
                    .then(params => {
                        expect(params.TableName).to.be.equal('MyTable');
                        expect(params.FilterExpression).to.be.equal('size (userId) = :userId');
                        expect(params.ExpressionAttributeValues).to.deep.equal({
                            ':userId': 20
                        });
                    });
            });


            it('Size Not Equal', () => {
                let db = new SimpleDB({ dummy: 'dummy' });
                return db.scan()
                    .tableName('MyTable')
                    .attribute('userId').size().notEqual(20)
                    .dryRun()
                    .then(params => {
                        expect(params.TableName).to.be.equal('MyTable');
                        expect(params.FilterExpression).to.be.equal('size (userId) <> :userId');
                        expect(params.ExpressionAttributeValues).to.deep.equal({
                            ':userId': 20
                        });
                    });
            });

            it('Size Less Than', () => {
                let db = new SimpleDB({ dummy: 'dummy' });
                return db.scan()
                    .tableName('MyTable')
                    .attribute('userId').size().lessThan(20)
                    .dryRun()
                    .then(params => {
                        expect(params.TableName).to.be.equal('MyTable');
                        expect(params.FilterExpression).to.be.equal('size (userId) < :userId');
                        expect(params.ExpressionAttributeValues).to.deep.equal({
                            ':userId': 20
                        });
                    });
            });

            it('Size Greater Than', () => {
                let db = new SimpleDB({ dummy: 'dummy' });
                return db.scan()
                    .tableName('MyTable')
                    .attribute('userId').size().greaterThan(20)
                    .dryRun()
                    .then(params => {
                        expect(params.TableName).to.be.equal('MyTable');
                        expect(params.FilterExpression).to.be.equal('size (userId) > :userId');
                        expect(params.ExpressionAttributeValues).to.deep.equal({
                            ':userId': 20
                        });
                    });
            });

            it('Size Less Than or Equal', () => {
                let db = new SimpleDB({ dummy: 'dummy' });
                return db.scan()
                    .tableName('MyTable')
                    .attribute('userId').size().lessThanOrEqual(20)
                    .dryRun()
                    .then(params => {
                        expect(params.TableName).to.be.equal('MyTable');
                        expect(params.FilterExpression).to.be.equal('size (userId) <= :userId');
                        expect(params.ExpressionAttributeValues).to.deep.equal({
                            ':userId': 20
                        });
                    });
            });

            it('Size Greater Than or Equal', () => {
                let db = new SimpleDB({ dummy: 'dummy' });
                return db.scan()
                    .tableName('MyTable')
                    .attribute('userId').size().greaterThanOrEqual(20)
                    .dryRun()
                    .then(params => {
                        expect(params.TableName).to.be.equal('MyTable');
                        expect(params.FilterExpression).to.be.equal('size (userId) >= :userId');
                        expect(params.ExpressionAttributeValues).to.deep.equal({
                            ':userId': 20
                        });
                    });
            });

        });
    });    

    describe('Attribute Join Condition', () => { 
        it('Attribute AND', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').equal('john').and()
                .attribute('age').equal(40)
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('userId = :userId AND age = :age');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'john', ':age': 40 });
                });  
        }).timeout(5000);

        it('Attribute OR', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.scan()
                .tableName('MyTable')
                .attribute('userId').equal('john').or()
                .attribute('age').equal(40)
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.FilterExpression).to.be.equal('userId = :userId OR age = :age');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'john', ':age': 40 });
                });  
        }).timeout(5000);
    });

    describe('Projection', () => {
        it('Projection as Array', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.query()
                .tableName('MyTable')
                .primaryKey('userId').equal('abc')
                .sortKey('createdTime').equal(123)
                .select(['a','b','c'])
                .dryRun()
                .then(params => {
                    console.log(params);
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.KeyConditionExpression).to.be.equal('userId = :userId AND createdTime = :createdTime');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'abc', ':createdTime': 123 });
                    expect(params.ProjectionExpression).to.be.equal('a, b, c');
                });
        }).timeout(5000);

        it('Projection as String', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.query()
                .tableName('MyTable')
                .primaryKey('userId').equal('abc')
                .sortKey('createdTime', 123, '=')
                .select('a,b,c')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.KeyConditionExpression).to.be.equal('userId = :userId AND createdTime = :createdTime');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'abc', ':createdTime': 123 });
                    expect(params.ProjectionExpression).to.be.equal('a,b,c');       
                    expect(params.ConsistentRead).to.not.be.equal(true);                    
                });
        }).timeout(5000);
    });
    
    describe('Secondary Index', () => {
        it('With Consistent Read', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.query()
                .tableName('MyTable')
                .primaryKey('userId').equal('abc')
                .indexName('MySecondaryIndex')
                .sortKey('createdTime', 123, '=')
                .isConsistentRead()
                .select('a,b,c')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.IndexName).to.be.equal('MySecondaryIndex');                    
                    expect(params.KeyConditionExpression).to.be.equal('userId = :userId AND createdTime = :createdTime');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'abc', ':createdTime': 123 });
                    expect(params.ProjectionExpression).to.be.equal('a,b,c');     
                    expect(params.ConsistentRead).to.be.equal(true);
                });
        }).timeout(5000);

        it('With ReturnedConsumedCapacity', () => {
            let db = new SimpleDB({ dummy: 'dummy' });
            return db.query()
                .tableName('MyTable')
                .primaryKey('userId').equal('abc')
                .indexName('MySecondaryIndex')
                .sortKey('createdTime', 123, '=')
                .isConsistentRead()
                .returnedConsumedCapacity('INDEXES')
                .select('a,b,c')
                .dryRun()
                .then(params => {
                    expect(params.TableName).to.be.equal('MyTable');
                    expect(params.IndexName).to.be.equal('MySecondaryIndex');                    
                    expect(params.KeyConditionExpression).to.be.equal('userId = :userId AND createdTime = :createdTime');
                    expect(params.ExpressionAttributeValues).to.deep.equal({ ':userId': 'abc', ':createdTime': 123 });
                    expect(params.ProjectionExpression).to.be.equal('a,b,c');     
                    expect(params.ConsistentRead).to.be.equal(true);
                    expect(params.ReturnConsumedCapacity).to.be.equal('INDEXES');
                });
        }).timeout(5000);

    });        

  
});