/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var events = require('events');
var moray = require('moray');
var util = require('util');
var stream = require('stream');
var lstream = require('lstream');
var vstream = require('vstream');
var VE = require('verror').VError;

var BatchStream = require('./batch_stream').BatchStream;



///--- Globals

var MANTA_DELETE_BUCKET = 'manta_delete_log';
var MANTA_FINALIZING_BUCKET = 'manta_uploads';

var MORAY_CONNECT_TIMEOUT = 10000;
var MORAY_PORT = 2020;



///--- Object

/**
 * This class will connect to Moray and delete the records from the
 * manta_delete_log table.
 */
function MorayCleaner(opts, listener) {
        assert.object(opts, 'opts');
        assert.object(opts.log, 'opts.log');
        assert.number(opts.batchSize, 'opts.batchSize');

        var self = this;
        this.log = opts.log;
        this.batchSize = opts.batchSize;
        this.morayClients = {};
        this.rowsDeleted = 0;
        this.rowsAlreadyDeleted = 0;

        if (listener) {
                self.addListener('error', listener);
        }
}
util.inherits(MorayCleaner, events.EventEmitter);
module.exports = MorayCleaner;



///--- Helpers

function deleteFromMoray(opts, cb) {
        var self = opts.self;
        var lines = opts.lines;
        var expectedShard = opts.expectedShard;
        var bucket = opts.bucket;
        var client = opts.client;
        var ms = 'moray';

        var log = self.log;

        var keys = [];
        for (var i = 0; i < lines.length; ++i) {
                var line = lines[i];
                log.debug({ line: line }, 'Processing line.');
                var parts = line.split('\t');
                if (parts.length < 4) {
                        log.error({
                                line: line
                        }, 'Is supposed to be a moray gc line,');
                        cb(VE({ info: { line: line, shard: expectedShard }},
                            'malformed input line'));
                        return;
                }

                var type = parts[0];
                var shard = parts[1];
                var objectId = parts[2];
                var time = parts[3];
                var key = '/' + objectId + '/' + time;

                if (type !== ms) {
                        log.info({ line: line },
                                 'Line is not expected type.  Skipping.');
                        continue;
                }

                if (expectedShard !== shard) {
                        log.info({ shard: shard, expectedShard: expectedShard },
                                 'Skipping line because it isnt for the ' +
                                 'expected shard.');
                        continue;
                }

                keys.push(key);
        }

        if (keys.length < 1) {
                return (cb());
        }

        var filter = '(|';
        for (i = 0; i < keys.length; ++ i) {
                filter += '(_key=' + keys[i] + ')';
        }
        filter += ')';

        var startDate = new Date();
        client.deleteMany(bucket, filter, function (err) {
                var endDate = new Date();
                var latency = endDate.getTime() - startDate.getTime();

                if (!err) {
                        self.rowsDeleted += keys.length;
                }

                log.info({
                        'audit': true,
                        'shard': expectedShard,
                        'lines': lines,
                        'keys': keys,
                        'rowsDeleted': err ? 0 : keys.length,
                        'error': err,
                        'latency': latency
                }, 'GC Audit.');

                cb();
        });
}



///--- Methods


MorayCleaner.prototype.getStats = function getStats() {
        return ({
                'rowsDeleted': this.rowsDeleted,
                'rowsAlreadyDeleted': this.rowsAlreadyDeleted
        });
};


MorayCleaner.prototype.getMorayClient = function getMorayClient(shard, cb) {
        var self = this;
        if (self.morayClients[shard]) {
                cb(self.morayClients[shard]);
                return;
        }

        var client = moray.createClient({
                log: self.log,
                connectTimeout: MORAY_CONNECT_TIMEOUT,
                host: shard,
                port: MORAY_PORT
        });

        //Yes, there's the possibility that there will be many of these.  Not
        // sure if I care yet.
        client.on('connect', function () {
                self.log.info({ shard: shard }, 'Connected to shard.');
                if (!self.morayClients[shard]) {
                        self.morayClients[shard] = client;
                }
                cb(self.morayClients[shard]);
        });
};


/**
 * Closes all the moray clients this cleaner has a handle on.
 */
MorayCleaner.prototype.close = function close(cb) {
        var self = this;
        for (var shard in self.morayClients) {
                var c = self.morayClients[shard];
                c.close();
        }
        cb();
};


/*
 * This stream is used as part of the implementation of
 * MorayCleaner#cleanStream().  It accepts batches of garbage collection
 * directives, as split into lines by "lstream" and subsequently assembled into
 * batches by a "BatchStream".  Each instance of this class is connected to a
 * parent MorayCleaner, through which it obtains a connection to the
 * appropriate Moray shard.  Each input line is parsed and the nominated
 * records are deleted using a bulk Moray operation.  Once all input has been
 * processed and all outstanding Moray operations have completed, the stream
 * emits the "workComplete" event.
 */
function MorayCleanerStream(opts) {
        var self = this;

        assert.object(opts, 'opts');
        assert.object(opts.log, 'opts.log');
        assert.string(opts.shard, 'opts.shard');
        assert.string(opts.bucket, 'opts.bucket');
        assert.string(opts.object, 'opts.object');
        assert.object(opts.parent, 'opts.parent');

        assert.ok((opts.bucket === MANTA_DELETE_BUCKET) ||
                  (opts.bucket === MANTA_FINALIZING_BUCKET));

        stream.Writable.call(this, {
                objectMode: true,
                highWaterMark: 0
        });

        self.mcs_log = opts.log;
        self.mcs_shard = opts.shard;
        self.mcs_bucket = opts.bucket;
        self.mcs_object = opts.object;
        self.mcs_parent = opts.parent;
        self.mcs_client = null;

        self.mcs_inputComplete = false;
        self.mcs_inCommit = false;
        self.mcs_pendingCommit = null;

        self.on('finish', function () {
                self.mcs_inputComplete = true;

                self.mcs_log.debug('finish event');

                setImmediate(self.emit.bind(self, 'workComplete'));
        });

        self.mcs_parent.getMorayClient(self.mcs_shard, function (client) {
                self.mcs_log.debug('getMorayClient ok');
                self.mcs_client = client;

                if (self.mcs_pendingCommit !== null) {
                        var pc = self.mcs_pendingCommit;
                        self.mcs_pendingCommit = null;

                        self.mcsCommit(pc.pc_batch, pc.pc_callback);
                }
        });
}
util.inherits(MorayCleanerStream, stream.Writable);

MorayCleanerStream.prototype.mcsCommit = function mcsCommit(batch, done) {
        var self = this;

        assert.object(batch, 'batch');
        assert.ok(Array.isArray(batch.entries), 'batch.entries');
        assert.func(done, 'done');

        if (batch.entries.length < 1) {
                setImmediate(done);
                return;
        }

        if (self.mcs_client === null) {
                /*
                 * We need to wait for the Moray client before we try to
                 * perform any database operations.
                 */
                assert.strictEqual(self.mcs_pendingCommit, null,
                    'a commit is already pending!');
                self.mcs_pendingCommit = {
                        pc_batch: batch,
                        pc_callback: done
                };
                return;
        }

        assert.strictEqual(self.mcs_inCommit, false, 'mcsCommit re-entry');
        self.mcs_inCommit = true;

        deleteFromMoray({
                self: self.mcs_parent,
                lines: batch.entries,
                expectedShard: self.mcs_shard,
                bucket: self.mcs_bucket,
                client: self.mcs_client
        }, function (err) {
                if (err) {
                        done(VE(err, 'deleteFromMoray("%s", "%s")',
                            self.mcs_shard, self.mcs_object));
                        return;
                }

                self.mcs_inCommit = false;

                done();
        });
};

MorayCleanerStream.prototype._write = function mcsWrite(batch, _, done) {
        var self = this;

        self.mcsCommit(batch, done);
};

/*
 * Accepts a stream of newline delimited records of the form:
 *
 *   moray + [TAB] + [moray hostname] + [TAB] + [objectId] +
 *       [TAB] + [delete time]
 *
 * For each line, delete the matching object (if it exists) from the
 * "manta_delete_log" table of the Moray shard.
 */
MorayCleaner.prototype.cleanStream = function cleanStream(opts) {
        var self = this;
        var log = self.log.child({
                object: opts.object
        });

        log.debug('creating moray cleaner stream');

        var cleaner = new MorayCleanerStream({
                shard: opts.shard,
                bucket: opts.bucket,
                object: opts.object,
                parent: self,
                log: log
        });

        /*
         * Use PipelineStream to return a combination of several processing
         * stages: chunking lines in the input into per-line strings, then
         * collecting those strings into batches, then finally cleaning them
         * via batched Moray requests.
         */
        var mcs = new vstream.PipelineStream({
                streams: [
                        new lstream({ highWaterMark: 0 }),
                        new BatchStream({ batchSize: self.batchSize }),
                        cleaner
                ],
                streamOpts: {
                        highWaterMark: 0,
                        objectMode: true
                }
        });

        /*
         * Forward the "workComplete" event from the internal
         * MorayCleanerStream to the consumer of the combined PipelineStream
         * we return.
         */
        cleaner.on('workComplete', mcs.emit.bind(mcs, 'workComplete'));

        return (mcs);
};
