// Copyright (c) 2012, Joyent, Inc. All rights reserved.

//var assert = require('assert-plus');
var SchemaReader = require('./schema_reader');



///--- API

module.exports = {

        /**
         * Creates a new consistent hash ring over a bunch of moray shards.
         */
        createSchemaReader: function createSchemaReader(reader, listener) {
                //assertArgument(reader, 'object');
                //assertArgument(listener, 'object');

                var schemaReader = new SchemaReader(reader, listener);
                return (schemaReader);
        }
};
