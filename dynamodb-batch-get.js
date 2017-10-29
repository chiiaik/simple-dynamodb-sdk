const _ = require('lodash');
const common = require('./dynamodb-common');


function tableName(name) {
    if (!this._paramRequestItems) {
        this._paramRequestItems = {};
    }

    let requestItem = {}
    this._paramRequestItems[name] = requestItem;

    this.primaryKey = (keyName, value) => _primaryKey(this, requestItem, keyName, value);
    this.keys = (values) => _keys(this, requestItem, values);
    return this;
}

function _keys(self, requestItem, values) {
    if (!requestItem.Keys) {
        requestItem.Keys = []
    }

    values.forEach(value => {
        requestItem.Keys.push(value);
    });

    self.projectionExpression = (attributes) => projectionExpression(self, requestItem, attributes);
    self.isConsistentRead = () => isConsistentRead(self, requestItem);
    return self;
}
/**
 * Get Operations
 */
function _primaryKey(self, requestItem, keyName, value) {
    if (_.isNil(value)) {
        self.equal = (val) => {

            if (!requestItem.Keys) {
                requestItem.Keys = []
            }
        
            let keysObject = {};
            keysObject[keyName] = val;
            requestItem.Keys.push(keysObject)
            self.sortKey = (keyName, value) => _sortKey(self, keysObject, keyName);
            self.projectionExpression = (attributes) => projectionExpression(self, requestItem, attributes);
            self.isConsistentRead = () => isConsistentRead(self, requestItem);
            return self;
        }
        return self;
    }


    if (!requestItem.Keys) {
        requestItem.Keys = [];
    }

    let keysObject = {};
    keysObject[keyName] = val;
    requestItem.Keys.push(keysObject)
    self.sortKey = (keyName, value) => _sortKey(self, keysObject, keyName);
    self.projectionExpression = (attributes) => projectionExpression(self, requestItem, attributes);
    self.isConsistentRead = () => isConsistentRead(self, requestItem);
    return self;
}

function _sortKey(self, keysObject, keyName, value) {
    if (_.isNil(value)) {
        self.equal = (val) => {
            keysObject[keyName] = val;
            return self;
        }
        return self;
    }

    keysObject[keyName] = value;    
    return self;
}

/**
 * Specify the projection expression
 * @param {array} attributes Array of attributes, e.g. ['userId','firstName', ...]
 */
function projectionExpression(self, parent, attributes) {
    if (_.isNil(attributes)) {
        return self;
    }

    if (_.isString(attributes)) {
        parent.ProjectionExpression = attributes;
        return self;
    }

    parent.ProjectionExpression = attributes.reduce((acc, attribute) => {
        let expression = attribute + ',';
        if (_.isEmpty(acc)) {
            return expression;
        }
        else {
            return acc + ' ' + expression;
        }
    }, parent.ProjectionExpression).replace(/,\s*$/, '');

    return self;
}

/**
 * Setting ConsistentRead param
 */
function isConsistentRead(self, parent) {
    parent.ConsistentRead = true;
    return self;
}

/**
 * ReturnConsumedCapacity param
 * @param {String} value 'INDEXES' | 'TOTAL' | 'NONE'
 */
function returnConsumedCapacity(value) {
    this._returnedConsumedCapacity = value;
    return this;
}

function _makeParams(self) {
    let params = {};
    
    if (self._paramRequestItems) {
        params.RequestItems = self._paramRequestItems;
    }

    if (!_.isEmpty(self._returnedConsumedCapacity)) {
        params.ReturnConsumedCapacity = self._returnedConsumedCapacity;
    }

    return params;
}

function run() {
    let params = _makeParams(this);
    return this._db.remove(params);
}

function dryRun() {
    let params = _makeParams(this);
    // console.log(params);    
    return Promise.resolve(params);    
}

module.exports = {
    tableName,
    returnConsumedCapacity,
    run,
    dryRun,
}