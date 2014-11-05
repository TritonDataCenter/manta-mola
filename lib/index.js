/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var Auditor = require('./auditor');
var AuditRowTransformer = require('./audit_row_transformer');
var AuditSweeper = require('./audit_sweeper');
var common = require('./common');
var CruftCollector = require('./cruft_collector');
var CruftRowTransformer = require('./cruft_row_transformer');
var GarbageCollector = require('./garbage_collector');
var GcPgRowTransformer = require('./gc_pg_row_transformer');
var JobManager = require('./job_manager');
var MorayCleaner = require('./moray_cleaner');
var Rebalancer = require('./rebalancer');
var SchemaReader = require('./schema_reader');
var util = require('util');



///--- Create Methods

function createAuditor(opts, listener) {
        assert.object(opts, 'opts missing');
        assert.object(opts.reader, 'opts.reader missing');

        var auditor = new Auditor(opts, listener);
        return (auditor);
}


function createAuditRowTransformer(opts, listener) {
        assert.object(opts, 'opts missing');
        assert.object(opts.reader, 'opts.reader missing');
        assert.string(opts.mantaKey, 'opts.mantaKey missing');

        var auditRowTransformer = new AuditRowTransformer(opts, listener);
        return (auditRowTransformer);
}


function createAuditSweeper(opts) {
        assert.object(opts, 'opts missing');
        assert.object(opts.reader, 'opts.reader missing');

        var auditSweeper = new AuditSweeper(opts);
        return (auditSweeper);
}


function createCruftCollector(opts, listener) {
        assert.object(opts, 'opts missing');
        assert.object(opts.reader, 'opts.reader missing');

        var cruftCollector = new CruftCollector(opts, listener);
        return (cruftCollector);
}


function createCruftRowTransformer(opts, listener) {
        assert.object(opts, 'opts missing');
        assert.object(opts.reader, 'opts.reader missing');
        assert.string(opts.mantaKey, 'opts.mantaKey missing');
        assert.number(opts.filterTimestamp, 'opts.filterTimestamp missing');

        var cruftRowTransformer = new CruftRowTransformer(opts, listener);
        return (cruftRowTransformer);
}


function createGarbageCollector(opts, listener) {
        assert.object(opts.reader);
        if (opts.gracePeriodMillis) {
                assert.number(opts.gracePeriodMillis);
        }

        var garbageCollector = new GarbageCollector(opts, listener);
        return (garbageCollector);
}


function createJobManager(opts, mantaClient, log) {
        assert.object(opts);
        assert.object(mantaClient);

        var jobManager = new JobManager(opts, mantaClient, log);
        return (jobManager);
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


function createRebalancer(opts, listener) {
        assert.object(opts, 'opts missing');
        assert.object(opts.reader, 'opts.reader missing');
        assert.object(opts.sharks, 'opts.sharks missing');
        assert.string(opts.dir, 'opts.dir missing');

        var rebalancer = new Rebalancer(opts, listener);
        return (rebalancer);
}


function createSchemaReader(reader, listener) {
        assert.object(reader);

        var schemaReader = new SchemaReader(reader, listener);
        return (schemaReader);
}



///--- API

module.exports = {
        common: common,
        createAuditor: createAuditor,
        createAuditRowTransformer: createAuditRowTransformer,
        createAuditSweeper: createAuditSweeper,
        createCruftCollector: createCruftCollector,
        createCruftRowTransformer: createCruftRowTransformer,
        createGarbageCollector: createGarbageCollector,
        createGcPgRowTransformer: createGcPgRowTransformer,
        createJobManager: createJobManager,
        createMorayCleaner: createMorayCleaner,
        createRebalancer: createRebalancer,
        createSchemaReader: createSchemaReader
};
