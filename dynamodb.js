const DynamoDBHelper = require('dynamodb-helper');
const dbquery = require('./dynamodb-query');
const dbget = require('./dynamodb-get');
const dbscan = require('./dynamodb-scan');
const dbput = require('./dynamodb-put');
const dbupdate = require('./dynamodb-update');
const common = require('./dynamodb-common');
const _ = require('lodash');


function get() {
    let retval = {};
    Object.assign(retval, common);
    Object.assign(retval, dbget);
    retval._db = new DynamoDBHelper(this.config);
    return retval;
    
}

function query() {
    let retval = {};
    Object.assign(retval, common);
    Object.assign(retval, dbquery);
    retval._db = new DynamoDBHelper(this.config);
    return retval;
}

function scan() {
    let retval = {};
    Object.assign(retval, common);
    Object.assign(retval, dbscan);
    retval._db = new DynamoDBHelper(this.config);
    return retval;
}

function put() {
    let retval = {};
    Object.assign(retval, common);
    Object.assign(retval, dbput);
    retval._db = new DynamoDBHelper(this.config);
    return retval;
}

function update() {
    let retval = {};
    Object.assign(retval, common);
    Object.assign(retval, dbupdate);
    retval._db = new DynamoDBHelper(this.config);
    return retval;
}

function DynamoDB(config) {
    if (_.isEmpty(config)) {
        throw new Error('missing config');
    }
    this.config = config; 
}

DynamoDB.prototype = {
    query,
    get,
    scan,
    put,
    update
}

module.exports = DynamoDB;