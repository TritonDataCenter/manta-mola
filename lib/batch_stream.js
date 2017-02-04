/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2017 Joyent, Inc.
 */

var assert = require('assert-plus');
var util = require('util');
var stream = require('stream');


/*
 * This object mode Transform stream collects all input objects into
 * batches, releasing them as output only once the batch grows to
 * the specified size.
 *
 * The "opts" object has one property: "batchSize", the integer number
 * of objects to include in each batch.
 *
 * This stream is not prescriptive about the structure of input objects.
 * Each output object has two properties:
 *
 *      "batchId"       the ordinal number of this batch within the
 *                      stream, starting at an ID of 0.
 *
 *      "entries"       an array of input objects in the order they
 *                      were written to the stream.
 */
function BatchStream(opts) {
        var self = this;

        assert.object(opts, 'opts');
        assert.number(opts.batchSize, 'opts.batchSize');
        assert.ok(opts.batchSize >= 1 && opts.batchSize < 100000,
            'opts.batchSize must be in the range [1, 100000)');

        stream.Transform.call(this, {
                objectMode: true,
                highWaterMark: 0
        });

        self.bs_batchSize = opts.batchSize;
        self.bs_batch = [];
        self.bs_batchId = 0;
        self.bs_objectCount = 0;
}
util.inherits(BatchStream, stream.Transform);

BatchStream.prototype.bsCommit = function bsCommit(done) {
        var self = this;

        if (self.bs_batch.length > 0) {
                var batch = {
                        batchId: self.bs_batchId++,
                        entries: self.bs_batch
                };

                self.bs_batch = [];

                self.push(batch);
        }

        setImmediate(done);
};

BatchStream.prototype._transform = function bsTransform(obj, _, done) {
        var self = this;

        /*
         * Collect a batch of input objects.  We only pass objects on
         * once we have assembled a full batch.  If there is a final,
         * short batch, it will be emitted by our _flush() routine.
         */
        self.bs_batch.push(obj);
        self.bs_objectCount++;
        if (self.bs_batch.length < self.bs_batchSize) {
                setImmediate(done);
                return;
        }

        self.bsCommit(done);
};

BatchStream.prototype._flush = function bsFlush(done) {
        var self = this;

        self.bsCommit(done);
};


module.exports = {
        BatchStream: BatchStream
};
