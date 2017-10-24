const _ = require('lodash');

/**
 * Get Operations
 */
function primaryKey(keyName, value) {
    this._primaryKey = { name: keyName, value }
    return this;
}

function sortKey(keyName, value) {
    this._sortKey = { name: keyName, value }
    return this;
}

function isConsistentRead() {
    this._consistentRead = true;
    return this;
}

function _makeGetParams(self) {
    let params = {};
    params.TableName = self._tableName;
    if (self._indexName) {
        params.IndexName = self._indexName;
    }

    if (self._primaryKey) {
        params.Key = {}
        params.Key[self._primaryKey.name] = self._primaryKey.value;
    }

    if (self._sortKey) {
        if (_.isEmpty(params.Key)) {
            throw new Error('please specify primary key');
        }

        params.Key[self._sortKey.name] = self._sortKey.value;
    }

    if (self._consistentRead) {
        params.ConsistentRead = self._consistentRead;
    }

    if (self._projectionExpression) {
        params.ProjectionExpression = self._projectionExpression;
    }   

    return params;
}

function run() {
    let params = _makeGetParams(this);
    return this._db.get(params);
}

function dryRun() {
    let params = _makeGetParams(this);
    console.log(params);    
    return Promise.resolve();    
}

module.exports = {
    primaryKey,
    sortKey,
    isConsistentRead,
    run,
    dryRun,
}