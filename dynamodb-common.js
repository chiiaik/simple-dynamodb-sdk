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
    let expression = attribute.name + ' ' + attribute.operator + ' :' + attribute.name;
    return expression;
}

/**
 * 
 * @param {Attribute} attribute { name, operator }
 */
function makeBetweenExpression(attribute) {
    let expression = attribute.name + ' ' + attribute.operator + ' :' + attribute.name + 'Lower' + ' AND :' + attribute.name + 'Upper';
    return expression;
}

/**
 * 
 * @param {Attributes} attribute { name, values:  [lowervalue, upperValue] }
 */
function makeInExpression(attribute) {
    let expression = attribute.name + ' ' + attribute.operator + ' ';
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
    let expression = attribute.operator + ' (' + attribute.name + ', ' + ':' + attribute.name + ')';
    return expression;
}

function makeAttributeExistsOrNotExistsExpression(attribute) {
    let expression = attribute.operator + ' (' + attribute.name + ')';
    return expression;
}

function makeAttributeContainsExpression(attribute) {
    let expression = attribute.operator + ' (' + attribute.name + ', ' + ':' + attribute.name + ')';
    return expression;
}

function makeAttributeSizeExpression(attribute) {
    let expression = attribute.operator + ' (' + attribute.name + ') ' + attribute.comparator + ' :' + attribute.name + ')';
    return expression;
}

function makeAttributeTypeExpression(attribute) {
    let expression = attribute.operator + ' (' + attribute.name + ', ' + ':' + attribute.name + ')';
    return expression;
}

function makeConditionExpression(attributes, joinCondition, initialConditionExpression, initialAttributeValues) {
    if (_.isNil(attributes)) {
        return this;
    }

    if (!_.isArray(attributes)) {
        if (!_.isNil(attributes.name) && !_.isNil(attributes.value) && !_.isNil(attributes.operator)) {
            attributes = [attributes];
        }
        else if(!_.isNil(attributes.name) && !_.isNil(attributes.values) && !_.isNil(attributes.operator)){
            attributes = [attributes];
        }
    }

    if (_.isEmpty(joinCondition) && attributes.length > 1) {
        throw new Error('multiple condition expression require the join operator argument');
    }

   

    let conditionExpression = attributes.reduce((acc, attribute) => { 
        let expression = acc.expression ? acc.expression : '';
        let attributeValues = acc.attributeValues ? acc.attributeValues : {};

        if (isComparator(attribute.operator)) {
            let newExpression = makeComparatorExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attributeValues[':' + attribute.name] = attribute.value;
        }
        else if (attribute.operator === 'BETWEEN') {
            let newExpression = makeBetweenExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attributeValues[':' + attribute.name + 'Lower'] = attribute.values[0];
            attributeValues[':' + attribute.name + 'Upper'] = attribute.values[1];
        }
        else if (attribute.operator === 'IN') {
            let newExpression = makeInExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attribute.values.forEach((value, index) => {
                attributeValues[':' + attribute.name + index] = attribute.values[index];
            });
        }
        else if (attribute.operator === 'begins_with') {
            let newExpression = makeBeginsWithExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attributeValues[':' + attribute.name] = attribute.value;
        }
        else if (attribute.operator === 'attribute_exists' ||
            attribute.operator === 'attribute_not_exists'
        ) {
            let newExpression = makeAttributeExistsOrNotExistsExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
        }
        else if (attribute.operator === 'contains') {
            let newExpression = makeAttributeContainsExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attributeValues[':' + attribute.name] = attribute.value;            
        }
        else if (attribute.operator === 'attribute_type') {
            let newExpression = makeAttributeTypeExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attributeValues[':' + attribute.name] = attribute.value;            
        }
        else if (attribute.operator === 'size') {
            let newExpression = makeAttributeSizeExpression(attribute);
            expression = _.isEmpty(expression) ? newExpression : expression + ' ' + joinCondition + ' ' + newExpression;
            attributeValues[':' + attribute.name] = attribute.value;            
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
    
        let result = makeConditionExpression(attributes, jc, this._filterExpression, this._attributeValues);
        
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

function or() {
    this._joinCondition = 'OR';
    return this;
}

function and() {
    this._joinCondition = 'AND';
    return this;
}

module.exports = {
    tableName,
    indexName,
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
    or
}