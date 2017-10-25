const _ = require('lodash');
const common = require('./dynamodb-common');



function _makeParams(self) {
    let params = common.makeParams(self);

    

    return params;
}

function run() {
    let params = _makeParams(this);
    return this._db.scan(params);
}

function dryRun() {
    let params = _makeParams(this);
    // console.log(params);    
    return Promise.resolve(params);    
}

module.exports = {
    run,
    dryRun
}