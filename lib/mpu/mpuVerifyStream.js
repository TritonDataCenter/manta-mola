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

var mpuCommon = require('./common');

var mulrs = require('./mpuUnlinkLiveRecordStream');
var MULRS_TYPE_PART = mulrs.MULRS_TYPE_PART;
var MULRS_TYPE_UPLOADDIR = mulrs.MULRS_TYPE_UPLOADDIR;

/*
 * MpuVerifyStream: Given an input stream of a batch of upload records all
 * for the same MPU, verifies that this MPU is a valid candidate for
 * garbage collection.
 *
 * The MPU is a valid candidate for garbage collection if a finalizing
 * record exists for the MPU.
 */
function MpuVerifyStream(args) {
        assert.object(args, 'args');
        assert.object(args.log, 'args.log');

        stream.Transform.call(this, {
            objectMode: true,
            highWaterMark: 0
        });

        this.log = args.log;
}
util.inherits(MpuVerifyStream, stream.Transform);
module.exports = MpuVerifyStream;

/*
 * Based on the records present, ensures that this MPU is valid to be garbage
 * collected.
 *
 * Parameters:
 * - id: upload ID
 * - records: array of strings containing the related records
 *
 */
MpuVerifyStream.prototype.validateMPU = function validateMPU(id, records, cb) {
        assert.string(id, 'id');
        assert.arrayOfObject(records, 'records');

        var self = this;

        var uploadRecord, partRecords, finalizingRecord;
        var invalidBatch = false;

        records.forEach(function (r) {
                assert.ok(r instanceof mpuCommon.LiveRecord ||
                          r instanceof mpuCommon.FinalizingRecord);

                var rId = r.uploadId;
                if (id !== rId) {
                        self.log.error({
                                batchUploadId: id,
                                recordUploadId: rId
                        }, 'MPU records batch has records with different ' +
                           'upload IDs');
                        invalidBatch = true;
                }

                var mpuObject;
                if (r instanceof mpuCommon.FinalizingRecord) {
                        mpuObject = mpuCommon.MPUOBJ_FINALIZINGRECORD;
                } else {
                        if (r.type === mpuCommon.MPU_PART) {
                                mpuObject = mpuCommon.MPUOBJ_PART;
                        } else {
                                assert.ok(r.type === mpuCommon.MPU_UPLOADDIR);
                                mpuObject = mpuCommon.MPUOBJ_UPLOADDIR;
                        }
                }
                assert.ok(mpuObject === mpuCommon.MPUOBJ_FINALIZINGRECORD ||
                          mpuObject === mpuCommon.MPUOBJ_UPLOADDIR ||
                          mpuObject === mpuCommon.MPUOBJ_PART);

                if (mpuObject === mpuCommon.MPUOBJ_FINALIZINGRECORD) {
                        if (!finalizingRecord) {
                                finalizingRecord = r;
                        } else {
                                self.log.error({
                                        uploadId: id,
                                        record: r
                                }, 'multiple finalizing records found for ' +
                                   'the same upload ID');
                                invalidBatch = true;
                        }
                } else if (mpuObject === mpuCommon.MPUOBJ_UPLOADDIR) {
                        if (!uploadRecord) {
                                uploadRecord = r;
                        } else {
                                self.log.error({
                                        uploadId: id,
                                        record: r
                                }, 'multiple upload records found for the ' +
                                   'same upload ID');
                                invalidBatch = true;
                        }
                } else if (mpuObject === mpuCommon.MPUOBJ_PART) {
                        if (!partRecords) {
                                partRecords = [];
                        }

                        partRecords.push(r);
                } else {
                       self.log.error({
                                uploadId: id,
                                record: r
                       }, 'invalid MPU record (not a finalizing record, ' +
                          'upload record, or part record');
                        invalidBatch = true;
                }
        });

        if (partRecords && partRecords.length > 0 && !uploadRecord) {
                //self.log.error({
                        //uploadId: id
                //}, 'part records found, but no upload record');
                //invalidBatch = true;
        }

        if (!invalidBatch && finalizingRecord) {
                assert.ok(finalizingRecord);
                assert.optionalObject(uploadRecord);
                assert.optionalArrayOfObject(partRecords);

                self.push({
                        uploadId: id,
                        finalizingRecord: finalizingRecord,
                        uploadRecord: uploadRecord,
                        partRecords: partRecords
                });
        }

        setImmediate(cb);
};

MpuVerifyStream.prototype._transform = function mvsTransform(batch, _, cb) {
        var self = this;

        assert.object(batch, 'batch');
        assert.string(batch.uploadId, 'batch.uploadId');
        assert.arrayOfObject(batch.records, 'batch.records');

        self.validateMPU(batch.uploadId, batch.records, cb);
};

MpuVerifyStream.prototype._flush = function mvsFlush(cb) {
        setImmediate(cb);
};
