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