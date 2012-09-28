// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');
var events = require('events');
var carrier = require('carrier');



///--- API

/**
 * This is a schema reader.  Schemas come in the form of:
 *
 * OBJECT_TABLE_DEFINITION
 * VALUE
 * VALUE
 * ...
 *
 * Where the OBJECT_TABLE_DEFINITION is:
 * {
 *   name: "[table_name]",
 *   keys: ["column1", "column2", ..., "columnN"]
 * }
 *
 * And a VALUE is:
 * ["value1", "value2", ..., "valueN"]
 *
 * This library will stash the schema and create flat objects of the form:
 * {
 *   __table: "table_name",
 *   column1: "value1",
 *   column2: "value2",
 *   ...,
 *   columnN: "valueN"
 * }
 *
 * Emits:
 *   - 'object' - when an object is sucessfully parsed.
 *   - 'error'  - if an object cannot be parsed.
 *   - 'end'    - when the stream is done.
 */
function SchemaReader(reader, listener) {
        var self = this;
        var schema;
        var line_number = 0;
        self.carrier = carrier.carry(reader);

        if (listener) {
                self.addListener('object', listener);
        }

        self.carrier.on('line', function (line) {
                ++line_number;
                try {
                        var obj = JSON.parse(line);
                        if (!schema) {
                                schema = obj;
                        } else {
                                var trans = transformObject(schema, obj);
                                self.emit('object', trans);
                        }
                } catch (err) {
                        err.line_number = line_number;
                        self.emit('error', err);
                }
        });

        self.carrier.on('end', function () {
                self.emit('end');
        });
}

util.inherits(SchemaReader, events.EventEmitter);
module.exports = SchemaReader;



///--- Helpers

function transformObject(schema, values) {
        var table = schema.name;
        var keys = schema.keys;
        var obj = { '__table': table };
        values = values.entry;
        for (var i = 0; i < values.length; ++i) {
                obj[keys[i]] = transformValue(values[i]);
        }
        return (obj);
}


function transformValue(value) {
        //Parse Objects
        if (value.lastIndexOf('{', 0) === 0) {
                //Apparently JSON.parse will automagically unescape quotes.
                value = JSON.parse(value);
        }
        //Nulls
        else if (value === '\\N') {
                value = null;
        }
        return (value);
}
