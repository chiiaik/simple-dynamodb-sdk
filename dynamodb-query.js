const _ = require('lodash');
const common = require('./dynamodb-common');

/**
 * Specify the key condition expression
 * @param {array} attributes Array of expression attributes, e.g. { name: 'userId', value: '123', condition: '='}
 * @param {string} joinCondition If multiple attributes are present, join operator is required ['AND'|'OR']
 */
function keyConditionExpression(attributes, joinCondition) {
    let jc = joinCondition;
    if (_.isEmpty(jc)) {
        jc = this._joinCondition;
    }
    
    let result = common.makeConditionExpression(this, attributes, jc, this._keyConditionExpression, this._attributeValues);
    
    this._keyConditionExpression = result.conditionExpression;
    this._attributeValues = result.attributeValues;

    return this;
}



function primaryKey(keyName, value) {
    if (_.isNil(value)) {
        this.equal = (value) => {
            return this.keyConditionExpression([{ name: keyName, value: value, comparator: '=' }]);
        }
        return this;
    }
    return this.keyConditionExpression([{ name: keyName, value: value, comparator: '=' }]);
}

function sortKey(keyName, value, comparator, func) {
    if (_.isNil(keyName)) {
        this.descendingOrder = descendingOrder.bind(this);        
        this.ascendingOrder = ascendingOrder.bind(this);
        return this;    
    }

    if (!_.isNil(value)) {
        return _sortKey(this, keyName, value, comparator, func);
    }

    this.equal = (value) => _sortKey(this, keyName, value, '=');
    this.notEqual = (value) => _sortKey(this, keyName, value, '<>');
    this.lessThan = (value) => _sortKey(this, keyName, value, '<');
    this.greaterThan = (value) => _sortKey(this, keyName, value, '>');
    this.lessThanOrEqual = (value) => _sortKey(this, keyName, value, '<=');
    this.greaterThanOrEqual = (value) => _sortKey(this, keyName, value, '>=');
    this.between = (lower, upper) => _sortKey(this, keyName, [lower, upper], BETWEEN);
    this.beginsWith = (prefix) => _sortKey(this, keyName, prefix, null, 'begins_with');
        
    return this;
}

function sortKeyEqualTo(keyName, value) {
    return _sortKey(self, keyName, value, '=');
}

function sortKeyNotEqualTo(keyName, value) {
    return _sortKey(self, keyName, value, '<>');
}

function sortKeyLessThan(keyName, value) {
    return _sortKey(self, keyName, value, '<');
}

function sortKeyGreaterThan(keyName, value) {
    return _sortKey(self, keyName, value, '>');
}

function sortKeyLessThanOrEqualTo(keyName, value) {
    return _sortKey(self, keyName, value, '<=');
}

function sortKeyGreaterThanOrEqualTo(keyName, value) {
    return _sortKey(self, keyName, value, '>=');
}

function sortKeyBetween(keyName, lower, upper) {
    return _sortKey(self, keyName, [lower, upper], 'BETWEEN');
}

function sortKeyBeginsWith(keyName, prefix) {
    return _sortKey(self, keyName, prefix, null, 'begins_with');
}

function _sortKey(self, keyName, value, comparator, func) {
    if (_.isEmpty(self._keyConditionExpression)) {
        throw new Error('please specify the primary key first');
    }

    if (_.isEmpty(comparator) && _.isEmpty(func)) {
        throw new Error('please specify comparator or func for sortKey');
    }

    if (func === 'begins_with') {
        if (_.isArray(value)) {
            throw new Error('begins_with operator accepts string as value');
        }

        return self.keyConditionExpression([{ name: keyName, value: value, operator: operator }], 'AND');        
    }
    else if (common.isComparator(comparator)) {
        if (_.isArray(value)) {
            throw new Error('only single value accepted for comparator =, <>, <, <=, >, >=');
        }
        return self.keyConditionExpression([{ name: keyName, value: value, comparator: comparator }], 'AND');
    }    
    else if(comparator === 'BETWEEN') {
        if (!_.isArray(value) || value.length < 2) {
            throw new Error('please specify 2 values in an array for the BETWEEN operation');
        }

        let attributes = { name: keyName, values: value, operator: operator }
        let result = common.makeConditionExpression(self, attributes, 'AND', self._keyConditionExpression, self._attributeValues);
        self._keyConditionExpression = result.conditionExpression;
        self._attributeValues = result.attributeValues;
        
        return self;
    }
    else {
        throw new Error('sort key condition expression only supports comparator, BETWEEN, and begins_with function (http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html)');
    }
}

/**
 * Set query returned data to be sorted in ascending order (default)
 */
function ascendingOrder() {
    this._paramScanIndexForward = true;
    return this;
}

/**
 * Set query returned data to be sorted in descending order 
 */
function descendingOrder() {
    this._paramScanIndexForward = false;
    return this;
}

function _makeQueryParams(self) {
    let params = common.makeParams(self);

    if (self._keyConditionExpression) {
        params.KeyConditionExpression = self._keyConditionExpression;
    }    

    if (!_.isNil(self._paramScanIndexForward)) {
        params.ScanIndexForward = self._paramScanIndexForward;
    }

    return params;
}



/**
 * Run the query
 */
function run() {
    let params = _makeQueryParams(this);
    return this._db.query(params);
}

function dryRun() {
    let params = _makeQueryParams(this);
    // console.log(params);    
    return Promise.resolve(params);
}



module.exports = {
    run,
    dryRun,
    keyConditionExpression,
    keyCondition: keyConditionExpression,

    /* Primary Key Methods */
    primaryKey,
    
    /* Sort Key Methods */
    sortKey,
    sortKeyBeginsWith,
    sortKeyBetween,
    sortKeyEqualTo,
    sortKeyGreaterThan,
    sortKeyGreaterThanOrEqualTo,
    sortKeyLessThan,
    sortKeyLessThanOrEqualTo,
    sortKeyNotEqualTo,

    
}

