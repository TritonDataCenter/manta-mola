// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var AuditRowTransformer = require('./audit_row_transformer');
var GarbageCollector = require('./garbage_collector');
var GcPgRowTransformer = require('./gc_pg_row_transformer');
var MorayCleaner = require('./moray_cleaner');
var SchemaReader = require('./schema_reader');
var util = require('util');



///--- Create Methods

function createAuditRowTransformer(opts, listener) {
        assert.object(opts, 'opts missing');
        assert.object(opts.reader, 'opts.reader missing');
        assert.string(opts.mantaKey, 'opts.mantaKey missing');

        var auditRowTransformer = new AuditRowTransformer(opts, listener);
        return (auditRowTransformer);
}


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
        createAuditRowTransformer: createAuditRowTransformer,
        createGarbageCollector: createGarbageCollector,
        createGcPgRowTransformer: createGcPgRowTransformer,
        createMorayCleaner: createMorayCleaner,
        createSchemaReader: createSchemaReader
};
