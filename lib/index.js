// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var GarbageCollector = require('./garbage_collector');
var PgRowTransformer = require('./pg_row_transformer');
var SchemaReader = require('./schema_reader');
var util = require('util');



///--- Create Methods

function createSchemaReader(reader, listener) {
        assert.object(reader);

        var schemaReader = new SchemaReader(reader, listener);
        return (schemaReader);
}

function createPgRowTransformer(opts, listener) {
        assert.object(opts, 'opts missing');
        assert.object(opts.reader, 'opts.reader missing');
        assert.ok(util.isDate(opts.dump_date),
                  'opts.dump_date isnt Date');
        assert.ok(util.isDate(opts.least_dump_date),
                  'opts.least_dump_date isnt Date');
        assert.string(opts.moray_hostname, 'Moray hostname missing');

        var pgRowTransformer = new PgRowTransformer(opts, listener);
        return (pgRowTransformer);
}

function createGarbageCollector(reader, listener) {
        assert.object(reader);

        var garbageCollector = new GarbageCollector(reader, listener);
        return (garbageCollector);
}



///--- API

module.exports = {

        createSchemaReader: createSchemaReader,

        createPgRowTransformer: createPgRowTransformer,

        createGarbageCollector: createGarbageCollector
};
