/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var mahi = require('mahi');
var stream = require('stream');
var util = require('util');
var vasync = require('vasync');

var sprintf = util.format;

var mpuCommon = require('./common');

var MULRS_TYPE_PART = 'partRecords';
var MULRS_TYPE_UPLOADDIR = 'uploadRecord';


/*
 * MpuUnlinkStream: Unlinks live records in Manta as part of the MPU garbage
 * collection process. Depending on the arguments to its constructor, this
 * stream will unlink a collection of parts in an upload directory or the upload
 * directory itself.
 *
 * Parameters:
 *  - args: an options block with the following required arguments:
 *      - log:
 *      - type:
 *      - mantaClient:
 *      - mahiClient:
 *
 *    and the following optional arguments:
 *      - dryRun:
 *      - verbose:
 */
function MpuUnlinkLiveRecordStream(args) {
        assert.object(args, 'args');
        assert.object(args.log, 'args.log');
        assert.string(args.type, 'args.type');
        assert.ok(args.type === MULRS_TYPE_PART ||
                args.type === MULRS_TYPE_UPLOADDIR);
        assert.object(args.mantaClient, 'args.mantaClient');
        assert.object(args.mahiClient, 'args.mahiClient');
        assert.optionalBool(args.dryRun, 'args.dryRun');
        assert.optionalBool(args.verbose, 'args.verbose');

        stream.Transform.call(this, {
                objectMode: true,
                highWaterMark: 0
        });

        this.log = args.log;
        this.mantaClient = args.mantaClient;
        this.mahiClient = args.mahiClient;
        this.type = args.type;
        this.dryRun = args.dryRun;
        this.verbose = args.verbose;

        this.mulrs_numRecordsSeen = 0;
        this.mulrs_numRecordsUnlinked = 0;
        this.mulrs_numBatchesDropped = 0;
}
util.inherits(MpuUnlinkLiveRecordStream, stream.Transform);

MpuUnlinkLiveRecordStream.prototype._transform =
function mulrsWrite(batch, _, cb) {
        assert.object(batch, 'batch');
        assert.string(batch.uploadId, 'batch.uploadId');
        assert.object(batch.finalizingRecord, 'batch.finalizingRecord');
        assert.optionalObject(batch.uploadRecord, 'batch.uploadRecord');
        assert.optionalArrayOfObject(batch.partRecords, 'batch.partRecords');

        var self = this;
        if (!batch[self.type]) {
        assert.object(batch, 'batch');
                self.push(batch);
                setImmediate(cb);
                return;
        }

        self.mulrs_numRecordsSeen++;

        /*
         * Moray stores a normalized key, but we will need the account
         * associated with each MPU to remove the file through the front door.
         */
        var uuid, account;
        assert.ok(batch.uploadRecord, 'batch must have an upload record');
        var f = batch.finalizingRecord.key.split(':');
        assert.ok(f.length == 2);
        var s = f[1].split('/');
        assert(s.length >= 2);
        uuid = s[1];

        self.mahiClient.getAccountById(uuid, function (err, info) {
                /*
                 * If we can't resolve the account information, we will have to
                 * drop this batch, as there's no way for us to remove the
                 * records from the front door.
                 *
                 * Unfortunately, in the case where we cannot resolve a UUID
                 * because the user has been deleted, any non-garbage collected
                 * MPUs cannot be cleaned up by the normal cleanup stream
                 * mechanism.
                 */
                if (err) {
                        self.log.error({
                                uploadId: batch.uploadId,
                                accountUuid: uuid,
                                err: err
                        }, 'could not fetch account uuid->name mapping');

                        self.mulrs_numBatchesDropped++;

                        setImmediate(cb);
                        return;
                }

                account = info.account.login;
                assert.string(account, 'account');

                var inputs;
                if (self.type === MULRS_TYPE_UPLOADDIR) {
                        inputs = [ batch.uploadRecord ];
                } else {
                        assert.ok(self.type === MULRS_TYPE_PART,
                                sprintf('invalid type: \"%s\"', self.type));
                        inputs = batch.partRecords;
                }
                assert.arrayOfObject(inputs, 'inputs');

                var opts = {
                        query: {
                                allowMpuDeletes: true
                        }
                };

                function unlink(p, ucb) {
                        self.mantaClient.unlink(p, opts, function (err2, res) {
                                if (err2) {
                                        errs.push(err2);
                                } else {
                                        self.mulrs_numRecordsUnlinked++;
                                }

                                ucb();
                        });
                }

                var errs = [];
                vasync.forEachParallel({
                        func: function optionalUnlinkLiveRecord(r, vcb) {
                                assert.string(r.key);
                                var mantaPath = r.key.replace(uuid, account);

                                if (self.verbose) {
                                        console.error('unlink ' + mantaPath);
                                }

                                if (!self.dryRun) {
                                        unlink(mantaPath, vcb);
                                } else {
                                        setImmediate(vcb);
                                }
                        },
                        inputs: inputs
                }, function (err3, results) {
                        if (err3) {
                                cb(err3);
                        } else {
                                if (errs.length > 0) {
                                        errs.forEach(function (e) {
                                                self.log.error({
                                                        id: batch.uploadId,
                                                        err: e
                                                }, 'unlink live record ' +
                                                   'stream failure');
                                        });

                                        self.mulrs_numBatchesDropped++;
                                } else {
                                        self.push(batch);
                                }

                                cb();
                        }
                });
        });
};

MpuUnlinkLiveRecordStream.prototype._flush = function mupsFlush(cb) {
        var self = this;

        self.log.info({
                numRecordsSeen: self.mulrs_numRecordsSeen,
                numRecordsUnlinked: self.mulrs_numRecordsUnlinked,
                numBatchesDropped: self.mulrs_numBatchesDropped
        }, 'done');

        setImmediate(cb);
};

module.exports = {
        MULRS_TYPE_PART: MULRS_TYPE_PART,
        MULRS_TYPE_UPLOADDIR: MULRS_TYPE_UPLOADDIR,

        MpuUnlinkLiveRecordStream: MpuUnlinkLiveRecordStream
};
