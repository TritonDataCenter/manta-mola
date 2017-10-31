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
        self.mbs_batch = [];            // array of record objects in the batch
        self.mbs_uploadId = null;       // current upload ID

        /*
         * Keep track of upload ids we've seen to ensure the input is, in fact,
         * in sorted order. We maintain some state about these to ease
         * port-mortem debugging if this stream throws because the input isn't
         * sorted, which could lead to metadata cruft from MPUs.
         */
        self.mbs_uploadIdMap = {};
        self.mbs_numRecords = 0;
}
util.inherits(MpuBatchStream, stream.Transform);

/*
 * Sends the current batch to the next stream, and resets the internal stream
 * state to prepare for a new batch.
 */
MpuBatchStream.prototype.mbs_commitBatch = function mbs_commitBatch() {
        var self = this;
        assert.string(self.mbs_uploadId);
        assert.ok(self.mbs_batch.length > 0, sprintf('no records for batch ' +
                        '(upload id %s)', self.mbs_uploadId));
        assert.object(self.mbs_uploadIdMap[self.mbs_uploadId]);

        var batch = {
                uploadId: self.mbs_uploadId,
                records: self.mbs_batch
        };

        self.mbs_uploadIdMap[self.mbs_uploadId].status = 'completed';
        self.push(batch);

        self.log.debug({
                uploadId: self.mbs_uploadId,
                batch: batch
        }, 'committed batch');

        self.mbs_uploadId = null;
        self.mbs_batch = [];
};

/*
 * Sets the upload ID for the current batch, and throws an exception if we've
 * seen this upload ID in a previous batch on this stream.
 *
 * Parameters:
 * - "id": upload ID for the new batch
 */
MpuBatchStream.prototype.mbs_createBatch = function mbs_createBatch(id) {
        assert.uuid(id, 'id');
        var self = this;

        assert.ok(self.mbs_uploadId === null, 'other batch in process');

        if (self.mbs_uploadIdMap[id]) {
                var msg = sprintf('Upload id \"%s\" has already been ' +
                        'processed. This is very bad. Some records may not ' +
                        'be garbage collected properly as a result.', id);
                self.log.fatal({
                        uploadId: id,
                        previousBatch: self.mbs_uploadIdMap[id]
                }, msg);
                throw (new Error(msg));
        } else {
                self.mbs_uploadId = id;
                self.mbs_uploadIdMap[id] = {
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
MpuBatchStream.prototype.mbs_batchPush = function mbs_batchPush(r) {
        assert.object(r, 'r');

        var self = this;
        self.mbs_batch.push(r);

        var b = self.mbs_uploadIdMap[r.uploadId];
        b.numRecords++;

        assert.ok(b.numRecords === self.mbs_batch.length,
                  sprintf('mismatch of batch count (%d) ' +
                          'and `numRecords` count (%d)',
                          self.mbs_batch.length,
                          b.numRecords));
};

MpuBatchStream.prototype._transform = function mbsTransform(record, _, cb) {
        assert.string(record, 'record');
        var self = this;

        var r = mpuCommon.recordToObject(record);
        assert.object(r, 'r');
        self.mbs_numRecords++;

        if (self.mbs_uploadId === null) {
                self.mbs_createBatch(r.uploadId);
        }

        /*
         * If this has the same upload ID as the previous upload, add it to the
         * batch; otherwise, commit the previous batch, start a new one, and add
         * the current record to the new batch.
         */
        if (self.mbs_uploadId === r.uploadId) {
                self.mbs_batchPush(r);
                setImmediate(cb);
        } else {
                self.mbs_commitBatch();

                self.mbs_createBatch(r.uploadId);
                self.mbs_batchPush(r);

                setImmediate(cb);
        }
};

MpuBatchStream.prototype._flush = function mbsFlush(cb) {
        var self = this;

        // Make sure to commit an outstanding batch.
        if (self.mbs_batch.length > 0) {
                self.mbs_commitBatch();
        }

        self.log.debug({
                batches: self.mbs_uploadIdMap
        }, 'completed batches');

        self.log.info({
                stats: self.getStats()
        }, 'done');

        setImmediate(cb);
};


MpuBatchStream.prototype.getStats = function getStats() {
        var self = this;

        return ({
                numBatches: Object.keys(self.mbs_uploadIdMap).length,
                numRecords: self.mbs_numRecords
        });
};

module.exports = MpuBatchStream;
