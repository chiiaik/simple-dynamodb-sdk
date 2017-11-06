const _ = require('lodash');
const common = require('./dynamodb-common');


function tableName(name) {
    if (!this._paramRequestItems) {
        this._paramRequestItems = {};
    }

    let requestItem = [];
    this._paramRequestItems[name] = requestItem;

    this['delete'] = (values) => _delete(this, requestItem, values);
    this.put = (values) => put(this, requestItem, values);
    return this;
}

function _delete(self, requestItem, values) {
    if (_.isArray(values)) {
        values.forEach((value => {
            requestItem.push({ DeleteRequest: value });            
        }));
        return self;
    }

    let deleteRequest = {};
    requestItem.push({ DeleteRequest: deleteRequest });
    self.primaryKey = (keyName) => _primaryKey(self, deleteRequest, keyName);
    return self;
}

function put(self, requestItem, values) {
    if (_.isArray(values)) {
        values.forEach((value => {
            let item = {};
            Object.assign(item, value);
            let putRequest = { Item: item }
            requestItem.push({ PutRequest: putRequest });            
        }));
        return self;
    }
    let item = {};
    let putRequest = { Item: item };
    requestItem.push({ PutRequest: putRequest });
    self.primaryKey = (keyName) => _attribute(self, item, keyName);
    self.attribute = (name) => _attribute(self, item, name);
    return self;
}

function _primaryKeys(self, parent, keysAndValues) {

}

function _primaryKey(self, parent, keyName) {
    self.equal = (val) => {
        
        if (!parent.Key) {
            parent.Key = {}
        }
    
        parent.Key[keyName] = val;

        self.attribute = (attributeName) => _attribute(self, parent, attributeName);
        return self;
    }

    return self;

}

function _attribute(self, parent, attributeName) {
    self.equal = (val) => {
        parent[attributeName] = val;
        return self;
    }
    return self;
}

/**
 * ReturnConsumedCapacity param
 * @param {String} value 'INDEXES' | 'TOTAL' | 'NONE'
 */
function returnConsumedCapacity(value) {
    this._paramReturnedConsumedCapacity = value;
    return this;
}

function returnItemCollectionMetricsSize() {
    this._paramReturnItemCollectionMetrics = 'SIZE';
    return this;
}

function _makeParams(self) {
    let params = {};
    
    if (self._paramRequestItems) {
        params.RequestItems = self._paramRequestItems;
    }

    if (!_.isEmpty(self._paramReturnedConsumedCapacity)) {
        params.ReturnConsumedCapacity = self._paramReturnedConsumedCapacity;
    }

    if (!_.isEmpty(self._paramReturnItemCollectionMetrics)) {
        params.ReturnItemCollectionMetrics = self._paramReturnItemCollectionMetrics;
    }

    return params;
}

function run() {
    let params = _makeParams(this);
    return this._db.batchWrite(params);
}

function dryRun() {
    let params = _makeParams(this);
    // console.log(params);    
    return Promise.resolve(params);    
}

module.exports = {
    tableName,
    returnConsumedCapacity,
    returnItemCollectionMetricsSize,
    run,
    dryRun,
}