const SimpleDB = require('../dynamodb');
const expect = require('chai').expect;

describe('DynamoDB Update Operation', () => {
    it('SET', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.update()
            .tableName('MyTable')
            .primaryKey('userId').equal('123')
            .sortKey('name').equal('alice')
            .set().attribute('age').equal(20)
            .set().attribute('job').equal('retired')
            .where().attribute('height').lessThan(150)
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('MyTable');
                expect(params.Key).to.deep.equal({ userId: '123', name: 'alice' });
                expect(params.UpdateExpression).to.be.equal('SET age = :age, job = :job');
                expect(params.ConditionExpression).to.be.equal('height < :height');
                expect(params.ExpressionAttributeValues).to.deep.equal({ ':height': 150, ':age': 20, ':job': 'retired' });                
            });
    });

    it('SET nested map attributes', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.update()
            .tableName('ProductCatalog')
            .primaryKey('Id').equal(123)
            .set().attribute('pr.5star[1]').equal(20)
            .set().attribute('PR.3star').equal('retired')
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('ProductCatalog');
                expect(params.Key).to.deep.equal({ Id: 123 });
                expect(params.UpdateExpression).to.be.equal('SET pr.5star[1] = :pr_5star_1_, PR.3star = :PR_3star');
                expect(params.ExpressionAttributeValues).to.deep.equal({ ':pr_5star_1_': 20, ':PR_3star': 'retired' });   
            });
    });

    it('SET append element to list', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.update()
            .tableName('ProductCatalog')
            .primaryKey('Id').equal(123)
            .set().attribute('ri').append(['Hacksaw', 'Screwdriver'])
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('ProductCatalog');
                expect(params.Key).to.deep.equal({ Id: 123 });
                expect(params.UpdateExpression).to.be.equal('SET ri = list_append(ri, :ri)');
                expect(params.ExpressionAttributeValues).to.deep.equal({ ':ri': [ 'Hacksaw', 'Screwdriver' ] });     
            });
    });

    it('SET prepend element to list', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.update()
            .tableName('ProductCatalog')
            .primaryKey('Id').equal(123)
            .set().attribute('ri').prepend(['Hacksaw', 'Screwdriver'])
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('ProductCatalog');
                expect(params.Key).to.deep.equal({ Id: 123 });
                expect(params.UpdateExpression).to.be.equal('SET ri = list_append(:ri, ri)');
                expect(params.ExpressionAttributeValues).to.deep.equal({ ':ri': [ 'Hacksaw', 'Screwdriver' ] });     
            });
    });

    it('SET if attribute not exist', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.update()
            .tableName('ProductCatalog')
            .primaryKey('Id').equal(123)
            .set().attribute('Price').ifNotExists().equal(200)
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('ProductCatalog');
                expect(params.Key).to.deep.equal({ Id: 123 });
                expect(params.UpdateExpression).to.be.equal('SET Price = if_not_exists(Price, :Price)');
                expect(params.ExpressionAttributeValues).to.deep.equal({ ':Price': 200 });     
            });
    });

    it('REMOVE', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.update()
            .tableName('ProductCatalog')
            .primaryKey('Id').equal(123)
            .remove().attribute('Brand')
            .attribute('InStock')
            .attribute('QuantityOnHand')
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('ProductCatalog');
                expect(params.Key).to.deep.equal({ Id: 123 });
                expect(params.UpdateExpression).to.be.equal('REMOVE Brand, InStock, QuantityOnHand');
            });
    });

    it('ADD', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.update()
            .tableName('ProductCatalog')
            .primaryKey('Id').equal(123)
            .add().attribute('QuantityOnHand').value(5)
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('ProductCatalog');
                expect(params.Key).to.deep.equal({ Id: 123 });
                expect(params.UpdateExpression).to.be.equal('ADD QuantityOnHand :QuantityOnHand');
                expect(params.ExpressionAttributeValues).to.deep.equal({ ':QuantityOnHand': 5 });                     
            });
    });

    it('ADD elements to a set', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.update()
            .tableName('ProductCatalog')
            .primaryKey('Id').equal(123)
            .add().attribute('Color').values(['Orange','Purple'])
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('ProductCatalog');
                expect(params.Key).to.deep.equal({ Id: 123 });
                expect(params.UpdateExpression).to.be.equal('ADD Color :Color');
                expect(params.ExpressionAttributeValues).to.deep.equal({ ':Color': ['Orange','Purple'] });                     
            });
    });

    it('DELETE elements from a set', () => {
        let db = new SimpleDB({ dummy: 'dummy' });
        return db.update()
            .tableName('ProductCatalog')
            .primaryKey('Id').equal(123)
            .delete().attribute('Color').values(['Yellow','Purple'])
            .dryRun()
            .then(params => {
                expect(params.TableName).to.be.equal('ProductCatalog');
                expect(params.Key).to.deep.equal({ Id: 123 });
                expect(params.UpdateExpression).to.be.equal('DELETE Color :Color');
                expect(params.ExpressionAttributeValues).to.deep.equal({ ':Color': ['Yellow','Purple'] });                     
            });
    });
});