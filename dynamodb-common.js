const _ = require('lodash');

/**
 * 
 * @param {string} name Name of table to run the query on 
 */
function tableName(name) {
    if (_.isNil(name)) {
        throw new Error('table name can\'t be empty');
    }

    this._tableName = name;
    return this;
}

/**
 * 
 * @param {string} name Name of secondary index to perform the query on
 */
function indexName(name) {
    if (_.isNil(name)) {
        return this;
    }

    this._indexName = name;
    return this;
}

/**
 * 
 * @param {Attribute} attribute { name, operator: '= | < | > | <= | >= | <>' }
 */
function makeComparatorExpression(attribute) {
    let expression = attribute.name + ' ' + attribute.comparator + ' :' + attribute.name;
    return expression;
}

/**
 * 
 * @param {Attribute} attribute { name, operator }
 */
function makeBetweenExpression(attribute) {
    let expression = attribute.name + ' ' + attribute.comparator + ' :' + attribute.name + 'Lower' + ' AND :' + attribute.name + 'Upper';
    return expression;
}

/**
 * 
 * @param {Attributes} attribute { name, values:  [lowervalue, upperValue] }
 */
function makeInExpression(attribute) {
    let expression = attribute.name + ' ' + attribute.comparator + ' ';
    let values = attribute.values;
    values.forEach((value, index) => {
        if (index === 0) {
            expression = expression + '(' + ':' + attribute.name + index + ',';
        }
        else {
            expression = expression + ':' + attribute.name + index + ',';
        }
    });

    expression = expression.replace(/,\s*$/, '') + ')';

    return expression;
}

function makeBeginsWithExpression(attribute) {
    let expression = attribute.func + ' (' + attribute.name + ', ' + ':' + attribute.name + ')';
    return expression;
}

function makeAttributeExistsOrNotExistsExpression(attribute) {
    let expression = attribute.func + ' (' + attribute.name + ')';
    return expression;
}

function makeAttributeContainsExpression(attribute) {
    let expression = attribute.func + ' (' + attribute.name + ', ' + ':' + attribute.name + ')';
    return expression;
}

function makeAttributeSizeExpression(attribute) {
    let expression = attribute.func + ' (' + attribute.name + ') ' + attribute.comparator + ' :' + attribute.name;
    return expression;
}

function makeAttributeTypeExpression(attribute) {
    let expression = attribute.func + ' (' + attribute.name + ', ' + ':' + attribute.name + ')';
    return expression;
}

function makeConditionExpression(self, attributes, joinCondition, initialConditionExpression, initialAttributeValues) {
    if (_.isNil(attributes)) {
        return self;
    }

    if (!_.isArray(attributes)) {
        if (!_.isNil(attributes.name) && !_.isNil(attributes.value) && (!_.isNil(attributes.comparator) || !_.isNil(attributes.func))) {
            attributes = [attributes];
        }
        else if(!_.isNil(attributes.name) && !_.isNil(attributes.values) && (!_.isNil(attributes.comparator) || !_.isNil(attributes.func))){
            attributes = [attributes];
        }
    }

    if (_.isEmpty(joinCondition) && attributes.length > 1) {
        throw new Error('multiple condition expression require the join operator argument');
    }

   

    let conditionExpression = attributes.reduce((acc, attribute) => { 
        let expression = acc.expression ? acc.expression : '';
        let attributeValues = acc.attributeValues ? acc.attributeValues : {};

        if (attribute.func === 'begins_with') {
            let newExpression = makeBeginsWithExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attributeValues[':' + attribute.name] = attribute.value;
        }
        else if (attribute.func === 'attribute_exists' ||
            attribute.func === 'attribute_not_exists'
        ) {
            let newExpression = makeAttributeExistsOrNotExistsExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
        }
        else if (attribute.func === 'contains') {
            let newExpression = makeAttributeContainsExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attributeValues[':' + attribute.name] = attribute.value;            
        }
        else if (attribute.func === 'attribute_type') {
            let newExpression = makeAttributeTypeExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attributeValues[':' + attribute.name] = attribute.value;            
        }
        else if (attribute.func === 'size') {
            let newExpression = makeAttributeSizeExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attributeValues[':' + attribute.name] = attribute.value;            
        }
        else if (isComparator(attribute.comparator)) {
            let newExpression = makeComparatorExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attributeValues[':' + attribute.name] = attribute.value;
        }
        else if (attribute.comparator === 'BETWEEN') {
            let newExpression = makeBetweenExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attributeValues[':' + attribute.name + 'Lower'] = attribute.values[0];
            attributeValues[':' + attribute.name + 'Upper'] = attribute.values[1];
        }
        else if (attribute.comparator === 'IN') {
            let newExpression = makeInExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attribute.values.forEach((value, index) => {
                attributeValues[':' + attribute.name + index] = attribute.values[index];
            });
        }
        

        return { expression, attributeValues };

    }, { expression: initialConditionExpression, attributeValues: initialAttributeValues });

    // if (!this._attributeNames) {
    //     this._attributeNames = {}
    // }

    // let attributeValues = _makeAttributeValues(attributes);

    return { conditionExpression: conditionExpression.expression, attributeValues: conditionExpression.attributeValues };
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

    if (_.isEmpty(jc)) {
        jc = 'AND';
    }
    
    let result = makeConditionExpression(this, attributes, jc, this._filterExpression, this._attributeValues);
    
    this._filterExpression = result.conditionExpression;
    this._attributeValues = result.attributeValues;

    return this;
}
    
/**
 * Specify the projection expression
 * @param {array} attributes Array of attributes, e.g. ['userId','firstName', ...]
 */
function projectionExpression(attributes) {
    if (_.isNil(attributes)) {
        return this;
    }

    if (_.isString(attributes)) {
        this._projectionExpression = attributes;
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

function isComparator(operator) {
    switch (operator) {
        case '<':
        case '>':
        case '=':
        case '<=':
        case '>=':
        case '<>':
            return true;
        default:
            return false;
    }
}

/**
 * Setting ConsistentRead param
 */
function isConsistentRead() {
    this._consistentRead = true;
    return this;
}

/**
 * ReturnConsumedCapacity param
 * @param {String} value 'INDEXES' | 'TOTAL' | 'NONE'
 */
function returnedConsumedCapacity(value) {
    this._returnedConsumedCapacity = value;
    return this;
}

/**
 * Limit the returned data
 * @param {Number} number Number specifiyong max return data
 */
function limit(number) {
    if (_.isNil(number)) {
        return this;
    }

    if (!_.isNumber(number)) {
        return this;
    }

    this._paramLimit = number;
    return this;
}

/**
 * Operate on a specific attribute other than the key
 * @param {String} name Name of the attribute
 */
function attribute(name) {
    this.equal = attributeValueComparatorOperation(this, name, '=');
    this.notEqual = attributeValueComparatorOperation(this, name, '<>');
    this.lessThan = attributeValueComparatorOperation(this, name, '<');
    this.greaterThan = attributeValueComparatorOperation(this, name, '>');    
    this.lessThanOrEqual = attributeValueComparatorOperation(this, name, '<=');
    this.greaterThanOrEqual = attributeValueComparatorOperation(this, name, '>=');
    this.between = attributeValueBetweenOperation(this, name, 'BETWEEN');
    this.in = attributeValueInOperation(this, name, 'IN');
    this.exists = attributeValueComparatorOperation(this, name, null, 'attribute_exists');
    this.notExists = attributeValueComparatorOperation(this, name, null, 'attribute_not_exists');
    this.beginsWith = attributeValueComparatorOperation(this, name, null, 'begins_with');
    this.contains = attributeValueComparatorOperation(this, name, null, 'contains');
    this.size = size(this, name);
    return this;
}

function size(self, name) {
    return () => {
        self.equal = attributeValueComparatorOperation(self, name, '=', 'size');
        self.notEqual = attributeValueComparatorOperation(self, name, '<>', 'size');
        self.lessThan = attributeValueComparatorOperation(self, name, '<', 'size');
        self.greaterThan = attributeValueComparatorOperation(self, name, '>', 'size');
        self.lessThanOrEqual = attributeValueComparatorOperation(self, name, '<=', 'size');
        self.greaterThanOrEqual = attributeValueComparatorOperation(self, name, '>=', 'size');

        delete self.size;
        delete self.exists;
        delete self.notExists;
        delete self.beginsWith;
        delete self.contains;
    
        return self;
    }    
}


function attributeValueComparatorOperation(self, name, comparator, func, joinCondition) {
    return (value) => {
        return self.filter([{
            name,
            value,
            func,
            comparator
        }], joinCondition);
    }    
}

function attributeValueBetweenOperation(self, name, comparator, joinCondition) {
    return (lower, upper) => {
        return self.filter([{
            name,
            values: [lower, upper],
            comparator
        }], joinCondition);
    }    
}

function attributeValueInOperation(self, name, comparator, func, joinCondition) {
    return (values) => {
        return self.filter([{
            name,
            values,
            func,
            comparator
        }], joinCondition);
    }    
}



function attributeValueEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        comparator: '='
    }], joinCondition);
}

function attributeValueNotEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        comparator: '<>'
    }], joinCondition);
}

function attributeValueLessThan(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        comparator: '<'
    }], joinCondition);
}

function attributeValueGreaterThan(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        comparator: '>'
    }], joinCondition);
}

function attributeValueLessThanOrEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        comparator: '<='
    }], joinCondition);
}

function attributeValueGreaterThanOrEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        comparator: '>='
    }], joinCondition);
}

function attributeValueBetween(name, lower, upper, joinCondition) {
    return this.filter([{
        name,
        values: [lower, upper],
        comparator: 'BETWEEN',
    }], joinCondition);
}

function attributeValueIn(name, values, joinCondition) {
    return this.filter([{
        name,
        values,
        comparator: 'IN',
    }], joinCondition);
}

function attributeExists(name, joinCondition) {
    return this.filter([{
        name,
        func: 'attribute_exists',
    }], joinCondition);
}

function attributeNotExists(name, joinCondition) {
    return this.filter([{
        name,
        func: 'attribute_not_exists',
    }], joinCondition);
}

function attributeBeginsWith(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        func: 'begins_with',
    }], joinCondition);
}

function attributeContains(name, value, joinCondition) {
    return this.filter([{
        name,
        value,
        func: 'contains',
    }], joinCondition);
}

function attributeSizeEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        value,        
        func: 'size',
        comparator: '=',
    }], joinCondition);
}

function attributeSizeLessThan(name, value, joinCondition) {
    return this.filter([{
        name,
        value,        
        func: 'size',
        comparator: '<',
    }], joinCondition);
}

function attributeSizeGreaterThan(name, value, joinCondition) {
    return this.filter([{
        name,
        value,        
        func: 'size',
        comparator: '>',
    }], joinCondition);
}

function attributeSizeLessThanOrEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        func: 'size',
        comparator: '<=',
    }], joinCondition);
}

function attributeSizeGreaterThanOrEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        value,        
        func: 'size',
        comparator: '>=',
    }], joinCondition);
}

function attributeSizeNotEqualTo(name, value, joinCondition) {
    return this.filter([{
        name,
        value,        
        func: 'size',
        comparator: '<>',
    }], joinCondition);
}

function or() {
    this._joinCondition = 'OR';
    return this;
}

function and() {
    this._joinCondition = 'AND';
    return this;
}

function makeParams(self) {
    let params = {};
    params.TableName = self._tableName;
    if (self._indexName) {
        params.IndexName = self._indexName;
    }

  

    // if (self._attributeNames) {
    //     params.ExpressionAttributeNames = self._attributeNames;
    // }    

    if (self._attributeValues) {
        params.ExpressionAttributeValues = self._attributeValues;
    }    

    if (self._projectionExpression) {
        params.ProjectionExpression = self._projectionExpression;
    }    

    if (self._filterExpression) {
        params.FilterExpression = self._filterExpression;
    }

    if (!_.isNil(self._consistentRead)) {
        params.ConsistentRead = self._consistentRead;
    }

    if (self._returnedConsumedCapacity) {
        params.ReturnConsumedCapacity = self._returnedConsumedCapacity;
    }

    if (!_.isNil(self._paramLimit)) {
        params.Limit = self._paramLimit;
    }

    return params;
}

module.exports = {
    tableName,
    indexName,
    makeParams,
    filter: filterExpression,
    select: projectionExpression, 
    projection: projectionExpression,
    makeAttributeContainsExpression,
    makeAttributeExistsOrNotExistsExpression,
    makeAttributeSizeExpression,
    makeAttributeTypeExpression,
    makeBeginsWithExpression,
    makeBetweenExpression,
    makeComparatorExpression,
    makeConditionExpression,
    makeInExpression,
    isComparator,
    and,
    or,
    isConsistentRead,
    returnedConsumedCapacity,

    /* Attribute Filters */
    attribute,
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