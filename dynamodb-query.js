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
    let result = common.makeConditionExpression(attributes, jc, this._keyConditionExpression, this._attributeValues);

    this._keyConditionExpression = result.conditionExpression;
    this._attributeValues = result.attributeValues;

    return this;
}

/**
 * Specify the fileter expression
 * @param {array} attributes Array of expression attributes, e.g. { name: 'userId', value: '123', condition: '='}
 * @param {string} joinOperator If multiple attributes are present, join operator is required ['AND'|'OR']
 */
function filterExpression(attributes, joinCondition) {

    let jc = joinCondition;
    if (_.isEmpty(jc)) {
        jc = this._joinCondition;
    }

    let result = common.makeConditionExpression(attributes, jc, this._filterExpression, this._attributeValues);
    
    this._filterExpression = result.conditionExpression;
    this._attributeValues = result.attributeValues;

    return this;
}

function primaryKey(keyName, value) {
    return this.keyConditionExpression([{ name: keyName, value: value, operator: '=' }]);
}


function sortKeyEqualTo(keyName, value) {
    return this.sortKey(keyName, value, '=');
}

function sortKeyNotEqualTo(keyName, value) {
    return this.sortKey(keyName, value, '<>');
}

function sortKeyLessThan(keyName, value) {
    return this.sortKey(keyName, value, '<');
}

function sortKeyGreaterThan(keyName, value) {
    return this.sortKey(keyName, value, '>');
}

function sortKeyLessThanOrEqualTo(keyName, value) {
    return this.sortKey(keyName, value, '<=');
}

function sortKeyGreaterThanOrEqualTo(keyName, value) {
    return this.sortKey(keyName, value, '>=');
}

function sortKeyBetween(keyName, lower, upper) {
    return this.sortKey(keyName, [lower, upper], 'BETWEEN');
}

function sortKeyBeginsWith(keyName, prefix) {
    return this.sortKey(keyName, prefix, 'begins_with');
}

function sortKey(keyName, value, operator) {
    if (_.isEmpty(this._keyConditionExpression)) {
        throw new Error('please specify the primary key first');
    }

    if (_.isEmpty(operator)) {
        throw new Error('please specify opeartor for sortKey');
    }
    /**
     * Comparison operators
     * keyName = value
     * keyName <> value
     * keyName < value | keyName > value | keyName <= value | keyName >= value
     *
     */
    if (common.isComparator(operator)) {
        if (_.isArray(value)) {
            throw new Error('only single value accepted for comparator =, <>, <, <=, >, >=');
        }
        return this.keyConditionExpression([{ name: keyName, value: value, operator: operator }], 'AND');
    }    
    else if(operator === 'BETWEEN') {
        if (!_.isArray(value) || value.length < 2) {
            throw new Error('please specify 2 values in an array for the BETWEEN operation');
        }

        let attributes = { name: keyName, values: value, operator: operator }
        let result = common.makeConditionExpression(attributes, 'AND', this._keyConditionExpression, this._attributeValues);
        
        this._keyConditionExpression = result.conditionExpression;
        this._attributeValues = result.attributeValues;
        
        return this;
    }
    else if (operator === 'begins_with') {
        if (_.isArray(value)) {
            throw new Error('begins_with operator accepts string as value');
        }

        return this.keyConditionExpression([{ name: keyName, value: value, operator: operator }], 'AND');        
    }
    else {
        throw new Error('sort key condition expression only supports comparator, BETWEEN, and begins_with function (http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html)');
    }
}

function attributeValueEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        operator: '='
    }], joinCondition);
}

function attributeValueNotEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        operator: '<>'
    }], joinCondition);
}

function attributeValueLessThan(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        operator: '<'
    }], joinCondition);
}

function attributeValueGreaterThan(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        operator: '>'
    }], joinCondition);
}

function attributeValueLessThanOrEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        operator: '<='
    }], joinCondition);
}

function attributeValueGreaterThanOrEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        operator: '>='
    }], joinCondition);
}

function attributeValueBetween(name, lower, upper, joinCondition) {
    return this.filter([{
        name,
        values: [lower, upper],
        operator: 'BETWEEN',
    }], joinCondition);
}

function attributeValueIn(name, values, joinCondition) {
    return this.filter([{
        name,
        values,
        operator: 'IN',
    }], joinCondition);
}

function attributeExists(name, joinCondition) {
    return this.filter([{
        name,
        operator: 'attribute_exists',
    }], joinCondition);
}

function attributeNotExists(name, joinCondition) {
    return this.filter([{
        name,
        operator: 'attribute_not_exists',
    }], joinCondition);
}

function attributeBeginsWith(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        operator: 'begins_with',
    }], joinCondition);
}

function attributeContains(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        operator: 'contains',
    }], joinCondition);
}

function attributeSizeEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        operator: 'size',
        comparator: '=',
    }], joinCondition);
}

function attributeSizeLessThan(name, value, joinCondition) {
    return this.filter([{
        name,
        operator: 'size',
        comparator: '<',
    }], joinCondition);
}

function attributeSizeGreaterThan(name, value, joinCondition) {
    return this.filter([{
        name,
        operator: 'size',
        comparator: '>',
    }], joinCondition);
}

function attributeSizeLessThanOrEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        operator: 'size',
        comparator: '<=',
    }], joinCondition);
}

function attributeSizeGreaterThanOrEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        operator: 'size',
        comparator: '>=',
    }], joinCondition);
}

function attributeSizeNotEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        operator: 'size',
        comparator: '<>',
    }], joinCondition);
}

/**
 * Specify the projection expression
 * @param {array} attributes Array of attributes, e.g. ['userId','firstName', ...]
 */
function projectionExpression(attributes) {
    if (_.isNil(attributes)) {
        return this;
    }

    this._projectionExpression = attributes.reduce((acc, attribute) => {
        let expression = attribute + ',';
        if (_.isEmpty(acc)) {
            return expression;
        }
        else {
            return acc + ' ' + expression;
        }
    }, this._projectionExpression).replace(/,\s*$/, '');

    return this;
}


function _makeQueryParams(self) {
    let params = {};
    params.TableName = self._tableName;
    if (self._indexName) {
        params.IndexName = self._indexName;
    }

    if (self._keyConditionExpression) {
        params.KeyConditionExpression = self._keyConditionExpression;
    }    

    if (self._attributeNames) {
        params.ExpressionAttributeNames = self._attributeNames;
    }    

    if (self._attributeValues) {
        params.ExpressionAttributeValues = self._attributeValues;
    }    

    if (self._projectionExpression) {
        params.ProjectionExpression = self._projectionExpression;
    }    

    if (self._filterExpression) {
        params.FilterExpression = self._filterExpression;
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
    console.log(params);    
    return Promise.resolve();
}



module.exports = {
    run,
    dryRun,
    keyConditionExpression,
    projectionExpression,
    filterExpression,
    keyCondition: keyConditionExpression,
    projection: projectionExpression,
    filter: filterExpression,
    select: projectionExpression,

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

    /* Attribute Filters */
    attributeBeginsWith,
    attributeContains,
    attributeExists,
    attributeNotExists,
    attributeSizeEqualTo,
    attributeSizeGreaterThan,
    attributeSizeGreaterThanOrEqualTo,
    attributeSizeLessThan,
    attributeSizeLessThanOrEqualTo,
    attributeSizeNotEqualTo,
    attributeValueBetween,
    attributeValueEqualTo,
    attributeValueGreaterThan,
    attributeValueGreaterThanOrEqualTo,
    attributeValueLessThan,
    attributeValueLessThanOrEqualTo,
    attributeValueNotEqualTo,
    attributeValueIn,
}

