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
                expect(params).to.deep.equal({
                    RequestItems: {
                        MyTable1: {
                            Keys: [
                                {
                                    userId: 'aaa',
                                    timestamp: 111
                                },
                                {
                                    userId: 'bbb',
                                    timestamp: 222
                                }
                            ],
                            ProjectionExpression: 'a,b,c'
                        },
                        MyTable2: {
                            Keys: [
                                {
                                    userId: 'ccc',
                                    timestamp: 333
                                },
                                {
                                    userId: 'ddd',
                                    timestamp: 444
                                }
                            ],
                            ProjectionExpression: 'd,e,f',
                            ConsistentRead: true
                        },
                    },
                    ReturnConsumedCapacity: 'TOTAL'
                });
               
            });
    });

    it('Array', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.batchGet()
            .tableName('MyTable1')
            .keys([{ userId: 'aaa', timestamp: 111},{ userId: 'bbb', timestamp: 222}])
            .projectionExpression('a,b,c')
            .isConsistentRead()
            .returnConsumedCapacity('TOTAL')
            .dryRun()
            .then(params => {
                expect(params).to.deep.equal({
                    RequestItems: {
                        MyTable1: {
                            Keys: [
                                {
                                    userId: 'aaa',
                                    timestamp: 111
                                },
                                {
                                    userId: 'bbb',
                                    timestamp: 222
                                }
                            ],
                            ProjectionExpression: 'a,b,c',
                            ConsistentRead: true
                        },
                    },
                    ReturnConsumedCapacity: 'TOTAL'
                });
               
            });
    });
});