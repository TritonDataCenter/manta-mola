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

var sprintf = util.format;

/*
 * MpuBatchStream: Collects all records in a stream for related mulitpart
 * uploads into a single batch, and passes this batch along to the next stream.
 *
 * NOTE: This stream assumes the input is sorted by upload ID and will throw an
 * exception if it encounters the same upload ID twice.
 *
 * Parameters:
 * - "args": an options object with the following required parameters:
 *      - "log": a bunyan logger
 */
function MpuBatchStream(args) {
        var self = this;

        assert.object(args, 'args');
        assert.object(args.log, 'args.log');

        stream.Transform.call(this, {
            objectMode: true,
            highWaterMark: 0
        });
        self.log = args.log;

        /* Current batch pointers */
        self.mpu_batch = [];            // array of record objects in the batch
        self.mpu_uploadId = null;       // current upload ID

        /*
         * Keep track of upload ids we've seen to ensure the input is, in fact,
         * in sorted order. We maintain some state about these to ease
         * port-mortem debugging if this stream throws because the input isn't
         * sorted, which could lead to metadata cruft from MPUs.
         */
        self.mpu_UPLOAD_IDS = {};
}
util.inherits(MpuBatchStream, stream.Transform);

/*
 * Sends the current batch to the next stream, and resets the internal stream
 * state to prepare for a new batch.
 */
MpuBatchStream.prototype.commitBatch = function commitBatch() {
        var self = this;
        assert.string(self.mpu_uploadId);
        assert.ok(self.mpu_batch.length > 0, sprintf('no records for batch ' +
                        '(upload id %s)', self.mpu_uploadId));
        assert.object(self.mpu_UPLOAD_IDS[self.mpu_uploadId]);

        var batch = {
                uploadId: self.mpu_uploadId,
                records: self.mpu_batch
        };

        self.mpu_UPLOAD_IDS[self.mpu_uploadId].status = 'completed';
        self.push(batch);

        self.mpu_uploadId = null;
        self.mpu_batch = [];
};

/*
 * Sets the upload ID for the current batch, and throws an exception if we've
 * seen this upload ID in a previous batch on this stream.
 *
 * Parameters:
 * - "id": upload ID for the new batch
 */
MpuBatchStream.prototype.createBatch = function createBatch(id) {
        assert.uuid(id, 'id');
        var self = this;

        assert.ok(self.mpu_uploadId === null, 'other batch in process');

        if (self.mpu_UPLOAD_IDS[id]) {
                var msg = sprintf('Upload id \"%s\" has already been ' +
                        'processed. This is very bad. Some records may not ' +
                        'be garbage collected properly as a result.', id);
                self.log.fatal({
                        uploadId: id,
                        previousBatch: self.mpu_UPLOAD_IDS[id]
                }, msg);
                throw (new Error(msg));
        } else {
                self.mpu_uploadId = id;
                self.mpu_UPLOAD_IDS[id] = {
                        uploadId: id,
                        status: 'processing',
                        numRecords: 0
                };
        }
};

/*
 * Push a record object onto the current batch.
 *
 * Parameters:
 *  - "r": record object to push
 */
MpuBatchStream.prototype.batchPush = function batchPush(r) {
        assert.object(r, 'r');

        var self = this;
        self.mpu_batch.push(r);

        var b = self.mpu_UPLOAD_IDS[r.uploadId];
        b.numRecords++;

        assert.ok(b.numRecords === self.mpu_batch.length,
                  sprintf('mismatch of batch count (%d) ' +
                          'and `numRecords` count (%d)',
                          self.mpu_batch.length,
                          b.numRecords));
};

MpuBatchStream.prototype._transform = function mbsTransform(record, _, cb) {
        assert.string(record, 'record');
        var self = this;

        var r = mpuCommon.recordToObject(record);
        assert.object(r, 'r');

        if (self.mpu_uploadId === null) {
                self.createBatch(r.uploadId);
        }

        /*
         * If this has the same upload ID as the previous upload, add it to the
         * batch; otherwise, commit the previous batch, start a new one, and add
         * the current record to the new batch.
         */
        if (self.mpu_uploadId === r.uploadId) {
                self.batchPush(r);
                setImmediate(cb);
        } else {
                self.commitBatch();

                self.createBatch(r.uploadId);
                self.batchPush(r);

                setImmediate(cb);
        }
};

MpuBatchStream.prototype._flush = function mbsFlush(cb) {
        var self = this;

        // Make sure to commit an outstanding batch.
        if (self.mpu_batch.length > 0) {
                self.commitBatch();
        }

        setImmediate(cb);
};

module.exports = MpuBatchStream;
