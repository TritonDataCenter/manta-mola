// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var GarbageCollector = require('./garbage_collector');
var MorayCleaner = require('./moray_cleaner');
var GcPgRowTransformer = require('./gc_pg_row_transformer');
var SchemaReader = require('./schema_reader');
var util = require('util');



///--- Create Methods

function createGarbageCollector(opts, listener) {
        assert.object(opts.reader);
        if (opts.gracePeriodMillis) {
                assert.number(opts.gracePeriodMillis);
        }

        var garbageCollector = new GarbageCollector(opts, listener);
        return (garbageCollector);
}


function createMorayCleaner(opts, listener) {
        assert.object(opts.log, 'opts.log');

        var morayCleaner = new MorayCleaner(opts, listener);
        return (morayCleaner);
}


function createGcPgRowTransformer(opts, listener) {
        assert.object(opts, 'opts missing');
        assert.object(opts.reader, 'opts.reader missing');
        assert.ok(util.isDate(opts.dumpDate),
                  'opts.dumpDate isnt Date');
        assert.ok(util.isDate(opts.earliestDumpDate),
                  'opts.earliestDumpDate isnt Date');
        assert.string(opts.morayHostname, 'Moray hostname missing');

        var gcPgRowTransformer = new GcPgRowTransformer(opts, listener);
        return (gcPgRowTransformer);
}


function createSchemaReader(reader, listener) {
        assert.object(reader);

        var schemaReader = new SchemaReader(reader, listener);
        return (schemaReader);
}



///--- API

module.exports = {
        createGarbageCollector: createGarbageCollector,
        createMorayCleaner: createMorayCleaner,
        createGcPgRowTransformer: createGcPgRowTransformer,
        createSchemaReader: createSchemaReader
};
