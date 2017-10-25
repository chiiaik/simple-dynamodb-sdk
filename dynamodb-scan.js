const _ = require('lodash');



function _makeParams(self) {
    let params = {};
    params.TableName = self._tableName;
    if (self._indexName) {
        params.IndexName = self._indexName;
    }

    if (self._projectionExpression) {
        params.ProjectionExpression = self._projectionExpression;
    }   

    if (self._filterExpression) {
        params.FilterExpression = self._filterExpression;
    }

    if (self._attributeNames) {
        params.ExpressionAttributeNames = self._attributeNames;
    } 

    if (self._attributeValues) {
        params.ExpressionAttributeValues = self._attributeValues;
    }  

    if (self._consistentRead) {
        params.ConsistentRead = self._consistentRead;
    }

    return params;
}

function run() {
    let params = _makeParams(this);
    return this._db.scan(params);
}

function dryRun() {
    let params = _makeParams(this);
    console.log(params);    
    return Promise.resolve();    
}

module.exports = {
    run,
    dryRun
}