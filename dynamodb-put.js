const common = require('./dynamodb-common');

function item(value) {
    this._paramItem = value;
    return this;
}



function attribute(name) {
    return this._attribute(this, this.condition.bind(this), name);    
}

function _makeParams(self) {
    let params = common.makeParams(self);

    if (self._paramItem) {
        params.Item = self._paramItem;
    }


    return params;
}

function run() {
    let params = _makeParams(this);
    return this._db.put(params);
}

function dryRun() {
    let params = _makeParams(this);
    // console.log(params);    
    return Promise.resolve(params);    
}

module.exports = {
    item,
    attribute,
    run,
    dryRun
}