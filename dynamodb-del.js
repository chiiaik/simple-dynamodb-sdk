const _ = require('lodash');
const common = require('./dynamodb-common');

/**
 * Get Operations
 */
function primaryKey(keyName, value) {
    if (_.isNil(value)) {
        this.equal = (val) => {
            this._primaryKey = { name: keyName, value: val };
        }
        return this;
    }

    this._primaryKey = { name: keyName, value }
    return this;
}

function sortKey(keyName, value) {
    if (_.isNil(value)) {
        this.equal = (val) => {
            this._sortKey = { name: keyName, val }
        }
        return this;
    }

    this._sortKey = { name: keyName, value }
    return this;
}


function _makeParams(self) {
    let params = common.makeParams(self);
    
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

    return params;
}

function run() {
    let params = _makeParams(this);
    return this._db.get(params);
}

function dryRun() {
    let params = _makeParams(this);
    // console.log(params);    
    return Promise.resolve(params);    
}

module.exports = {
    primaryKey,
    sortKey,
    run,
    dryRun,
}