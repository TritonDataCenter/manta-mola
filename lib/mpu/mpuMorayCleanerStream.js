/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var stream = require('stream');
var util = require('util');

var assert = require('assert-plus');
var moray = require('moray');

var mpuCommon = require('./common');


///--- Globals

var MORAY_CONNECT_TIMEOUT = 10000;
var MORAY_PORT = 2020;

/*
 * MpuMorayCleanerStream: Deletes the finalizing record for the MPU.
 */
function MpuMorayCleanerStream(args) {
        assert.object(args, 'args');
        assert.object(args.log, 'args.log');
        assert.optionalBool(args.dryRun, 'args.dryRun');
        assert.optionalBool(args.verbose, 'args.verbose');

        stream.Writable.call(this, {
            objectMode: true,
            highWaterMark: 0
        });

        this.log = args.log;
        this.morayClients = {};
        this.dryRun = args.dryRun;
        this.verbose = args.verbose;

        var self = this;

        self.mcs_numBatchesInput = 0;
        self.mcs_numRecordsDeleted = 0;
        self.mcs_numBatchesDropped = 0;

        self.on('finish', function () {
                for (var c in self.morayClients) {
                        var client = self.morayClients[c].client;
                        client.close();
                }

                self.log.info({
                        stats: self.getStats()
                }, 'done');
        });
}
util.inherits(MpuMorayCleanerStream, stream.Writable);
module.exports = MpuMorayCleanerStream;

MpuMorayCleanerStream.prototype.mcs_getMorayClient =
function mcs_getMorayClient(shard, cb) {
        assert.string(shard, 'shard');
        assert.func(cb, 'cb');

        var self = this;

        function onConnect() {
                self.log.info({ shard: shard }, 'Connected to shard.');
                self.morayClients[shard].connected = true;
                cb(self.morayClients[shard].client);
        }

        var cObj = self.morayClients[shard];
        if (cObj) {
                /*
                 * We've already created a client at this point, so we will pass
                 * it to the caller once it's connected.
                 */
                if (cObj.connected) {
                        cb(self.morayClients[shard].client);
                        return;
                } else {
                        assert.object(cObj.client, 'no client found');
                        cObj.client.once('connect', onConnect);
                        return;
                }
        }

        var client = moray.createClient({
                log: self.log,
                connectTimeout: MORAY_CONNECT_TIMEOUT,
                host: shard,
                port: MORAY_PORT
        });
        self.morayClients[shard] = {
                client: client,
                connected: false
        };

        client.once('connect', onConnect);
};

MpuMorayCleanerStream.prototype.mcs_deleteFinalizingRecord =
function mcs_deleteFinalizingRecord(shard, key, cb) {
        var self = this;

        self.mcs_getMorayClient(shard, function (client) {
                assert.object(client);
                client.delObject(mpuCommon.MPU_MORAY_BUCKET, key, cb);
        });
};

MpuMorayCleanerStream.prototype._write = function mmcsWrite(batch, _, cb) {
        assert.object(batch, 'batch');
        assert.string(batch.uploadId, 'batch.uploadId');
        assert.object(batch.finalizingRecord, 'batch.finalizingRecord');

        var fr = batch.finalizingRecord;
        assert.string(fr.uploadId, 'fr.uploadId');
        assert.ok(fr.uploadId === batch.uploadId, 'upload ID of finalizing ' +
                'record does not match batch uploadId');
        assert.string(fr.key, 'fr.key');
        assert.string(fr.shard, 'fr.shard');
        assert.object(fr.date, 'fr.date');
        assert.ok(fr.date instanceof Date, 'invalid date');

        var self = this;
        self.mcs_numBatchesInput++;

        if (self.verbose) {
                console.error('delObject ' + fr.key);
        }

        if (!self.dryRun) {
                self.mcs_deleteFinalizingRecord(fr.shard, fr.key,
                   function (err) {
                        if (err) {
                                /*
                                 * We don't want to throw an error here in case
                                 * this is an isolated problem, so log an error
                                 * and continue.
                                 */
                                self.log.error({
                                        uploadId: batch.uploadId,
                                        shard: fr.shard,
                                        key: fr.key,
                                        err: err
                                }, 'mpu moray cleaner stream failure');

                                self.mcs_numBatchesDropped++;
                        } else {
                                self.log.debug({
                                        key: fr.key,
                                        shard: fr.shard
                                }, 'delobject');

                                self.mcs_numRecordsDeleted++;
                        }

                        cb();
                });
        } else {
                cb();
        }
};

MpuMorayCleanerStream.prototype.getStats = function getStats() {
        var self = this;

        return ({
                numBatchesInput: self.mcs_numBatchesInput,
                numBatchesOutput: self.mcs_numBatchesInput -
                        self.mcs_numBatchesDropped,
                numRecordsDeleted: self.mcs_numRecordsDeleted,
                numBatchesDropped: self.mcs_numBatchesDropped
        });
};
