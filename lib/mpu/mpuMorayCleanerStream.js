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
        this.on('finish', function () {
                for (var c in self.morayClients) {
                        var client = self.morayClients[c];
                        client.close();
                }
        });
}
util.inherits(MpuMorayCleanerStream, stream.Writable);
module.exports = MpuMorayCleanerStream;

MpuMorayCleanerStream.prototype.getMorayClient =
function getMorayClient(shard, cb) {
        assert.string(shard, 'shard');
        assert.func(cb, 'cb');

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

        client.on('connect', function () {
                self.log.info({ shard: shard }, 'Connected to shard.');
                if (!self.morayClients[shard]) {
                        self.morayClients[shard] = client;
                }
                cb(self.morayClients[shard]);
        });
};

MpuMorayCleanerStream.prototype.deleteFinalizingRecord =
function deleteFinalizingRecord(shard, key, cb) {
        var self = this;

        self.getMorayClient(shard, function (client) {
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
        if (self.verbose) {
                console.error('delObject ' + fr.key);
        }

        if (!self.dryRun) {
                self.deleteFinalizingRecord(fr.shard, fr.key, function (err) {
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
                        } else {
                                self.log.info({
                                        key: fr.key,
                                        shard: fr.shard
                                }, 'delobject');
                        }

                        cb();
                });
        } else {
                cb();
        }
};