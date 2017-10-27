const common = require('./dynamodb-common');
const _ = require('lodash');



function makeParamKey(self, keyName, value) {
    if (_.isNil(self._paramKey)) {
        self._paramKey = {};
    }

    self._paramKey[keyName] = value;
    return self;
}

function primaryKey(keyName, value) {
    if (_.isNil(value)) {
        this.equal = (val) => {
            return makeParamKey(this, keyName, val);
        }
    }

    return makeParamKey(this, keyName, value);
}

function sortKey(keyName, value) {
    return this.primaryKey(keyName, value);
}



function set() {
    this.attribute = (name) => {
        if (!this._paramUpdateSetAttributeNames) {
            this._paramUpdateSetAttributeNames = [];
        }

        this._paramUpdateSetAttributeNames.push(name);

        this.equal = (value) => {
            if (!this._paramUpdateSetAttributeValues) {
                this._paramUpdateSetAttributeValues = [];
            }
    
            this._paramUpdateSetAttributeValues.push({ type: 'value', value });
            return this;
        }
    
        this.append = (value) => {
            if (!this._paramUpdateSetAttributeValues) {
                this._paramUpdateSetAttributeValues = [];
            }
    
            this._paramUpdateSetAttributeValues.push({ type: 'func', value, func: 'list_append' });
            return this;
        }
    
        this.prepend = (value) => {
            if (!this._paramUpdateSetAttributeValues) {
                this._paramUpdateSetAttributeValues = [];
            }
    
            this._paramUpdateSetAttributeValues.push({ type: 'func', value, func: 'list_append', arg: 'prepend' });
            return this;
        }
    
        this.ifNotExists = () => {
            this.equal = (value) => {
                if (!this._paramUpdateSetAttributeValues) {
                    this._paramUpdateSetAttributeValues = [];
                }
        
                this._paramUpdateSetAttributeValues.push({ type: 'func', value, func: 'if_not_exists' });
                return this;
                
            }
    
            return this;
         }

        return this;
    }
    
    return this;
}

function remove() {
    this.attribute = (name) => {
        if (!this._paramUpdateRemoveAttributeNames) {
            this._paramUpdateRemoveAttributeNames = [];
        }

        this._paramUpdateRemoveAttributeNames.push(name);
        return this;
    }
    return this;
}

function add() {
    this.attribute = (name) => {
        if (!this._paramUpdateAddAttributeNames) {
            this._paramUpdateAddAttributeNames = [];
        }

        this._paramUpdateAddAttributeNames.push(name);

        this.value = (value) => {
            if (!this._paramUpdateAddAttributeValues) {
                this._paramUpdateAddAttributeValues = [];
            }
    
            this._paramUpdateAddAttributeValues.push({ type: 'value', value });
            return this;
        }

        this.values = (values) => {
            if (!_.isArray(values)) {
                throw new Error('values has to be an array');
            }
            this.value(values);

            return this;
        }
        return this;
    }

    return this;
}

function del() {
    this.attribute = (name) => {
        if (!this._paramUpdateDeleteAttributeNames) {
            this._paramUpdateDeleteAttributeNames = [];
        }

        this._paramUpdateDeleteAttributeNames.push(name);

        this.value = (value) => {
            if (!this._paramUpdateDeleteAttributeValues) {
                this._paramUpdateDeleteAttributeValues = [];
            }
    
            this._paramUpdateDeleteAttributeValues.push({ type: 'value', value });
            return this;
        }

        this.values = (values) => {
            if (!_.isArray(values)) {
                throw new Error('values has to be an array');
            }
            this.value(values);

            return this;
        }
        return this;
    }

    return this;
}

function where() {
    this.attribute = (name) => common._attribute(this, this.condition.bind(this), name);
    return this;
}

function valueAliasFromName(name) {
    return name.replace(/[\[\]\.]/g, '_');
}

function makeUpdateSetExpression(self) {
    return self._paramUpdateSetAttributeNames.reduce((acc, name, index) => { 
        let retval;
        let attValue = self._paramUpdateSetAttributeValues[index];

        if (attValue.type === 'value') {
            if (index === 0) {
                retval = acc + ' ' + name + ' = :' + valueAliasFromName(name);
            }
            else {
                retval = acc + ', ' + name + ' = :' + valueAliasFromName(name);
            }
        
            if (!self._paramExpressionAttributeValues) {
                self._paramExpressionAttributeValues = {};
            }

            self._paramExpressionAttributeValues[':' + valueAliasFromName(name)] = attValue.value;
        }
        else if (attValue.type === 'func') {

            if (attValue.arg === 'prepend'){
                if (index === 0) {
                    retval = acc + ' ' + name + ' = ' + attValue.func + '(:' + valueAliasFromName(name) + ', ' + name + ')';
                }
                else {
                    retval = acc + ', ' + name + ' = ' + attValue.func + '(:' + valueAliasFromName(name) + ', ' + name + ')';
                }
            }
            else {
                if (index === 0) {
                    retval = acc + ' ' + name + ' = ' + attValue.func + '(' + name + ', :' + valueAliasFromName(name) + ')';
                }
                else {
                    retval = acc + ', ' + name + ' = ' + attValue.func + '(' + name + ', :' + valueAliasFromName(name) + ')';
                }
            }
        
            if (!self._paramExpressionAttributeValues) {
                self._paramExpressionAttributeValues = {};
            }

            self._paramExpressionAttributeValues[':' + valueAliasFromName(name)] = attValue.value;
        }

        return retval;
    }, 'SET');
}

function makeUpdateRemoveExpression(self) {
    return self._paramUpdateRemoveAttributeNames.reduce((acc, name, index) => { 
        let retval;

        if (index === 0) {
            retval = acc + ' ' + name;
        }
        else {
            retval = acc + ', ' + name;
        }
    
        return retval;
    }, 'REMOVE');
}

function makeUpdateAddExpression(self) {
    return self._paramUpdateAddAttributeNames.reduce((acc, name, index) => { 
        let retval;
        let attValue = self._paramUpdateAddAttributeValues[index];
        if (index === 0) {
            retval = acc + ' ' + name + ' :' + name;
        }
        else {
            retval = acc + ', ' + name + ' :' + name;
        }

        if (!self._paramExpressionAttributeValues) {
            self._paramExpressionAttributeValues = {};
        }

        self._paramExpressionAttributeValues[':' + valueAliasFromName(name)] = attValue.value;
    
        return retval;
    }, 'ADD');
}

function makeUpdateDeleteExpression(self) {
    return self._paramUpdateDeleteAttributeNames.reduce((acc, name, index) => { 
        let retval;
        let attValue = self._paramUpdateDeleteAttributeValues[index];
        if (index === 0) {
            retval = acc + ' ' + name + ' :' + name;
        }
        else {
            retval = acc + ', ' + name + ' :' + name;
        }

        if (!self._paramExpressionAttributeValues) {
            self._paramExpressionAttributeValues = {};
        }

        self._paramExpressionAttributeValues[':' + valueAliasFromName(name)] = attValue.value;
    
        return retval;
    }, 'DELETE');
}

function makeUpdateExpression(self) {
    if (self._paramUpdateSetAttributeNames && self._paramUpdateSetAttributeValues) {
        if (self._paramUpdateSetAttributeNames.length === self._paramUpdateSetAttributeValues.length) {
            return makeUpdateSetExpression(self);
        }
    }
    else if(self._paramUpdateRemoveAttributeNames){
        if (self._paramUpdateRemoveAttributeNames.length) {
            return makeUpdateRemoveExpression(self);
        }
    }
    else if (self._paramUpdateAddAttributeNames && self._paramUpdateAddAttributeValues) {
        if (self._paramUpdateAddAttributeNames.length === self._paramUpdateAddAttributeValues.length) {
            return makeUpdateAddExpression(self);
        }
    }
    else if (self._paramUpdateDeleteAttributeNames && self._paramUpdateDeleteAttributeValues) {
        if (self._paramUpdateDeleteAttributeNames.length === self._paramUpdateDeleteAttributeValues.length) {
            return makeUpdateDeleteExpression(self);
        }
    }
}

function _makeParams(self) {
    let updateExpression = makeUpdateExpression(self);
    
    let params = common.makeParams(self);
    
    if (self._paramKey) {
        params.Key = self._paramKey;
    }

    if (updateExpression) {
        params.UpdateExpression = updateExpression;
    }

    

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
    primaryKey,
    sortKey,
    set,
    remove,
    add,
    del,
    where,
    run,
    dryRun
}