/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var util = require('util');
var events = require('events');
var carrier = require('carrier');

var mpuCommon = require('./mpu/common');


///--- Globals

var DEFAULT_GRACE_PERIOD_MILLIS = 1000 * 60 * 60 * 24 * 2;  // 2 days

var sprintf = util.format;

///--- API

/*
 * Emits 'cleanup' events for all MPU records that should be garbage collected.
 *
 * This object is analogous to the GarbageCollector object for normal GC. In the
 * MPU case, only one type of event is emitted, as all records are cleaned up
 * with the same cleanup scripts, unlike the normal GC case.
 */
function MpuGarbageCollector(opts, listener) {
        assert.object(opts, 'opts');
        assert.object(opts.reader, 'opts.reader');
        assert.optionalObject(opts.listener, 'opts.listener');
        assert.optionalObject(opts.gcDate, 'opts.gcDate');

        var self = this;
        if (opts.gcDate) {
                self.gcDate = opts.gcDate;
        } else  {
                self.gcDate = Date.now();
        }

        var prev, curr, currFR;
        self.gracePeriodMillis = opts.gracePeriodMillis ||
                DEFAULT_GRACE_PERIOD_MILLIS;
        self.carrier = carrier.carry(opts.reader);

        if (listener) {
                self.addListener('mpuCleanup', listener);
        }

        self.carrier.on('line', function (line) {
                curr = mpuCommon.recordToObject(line);
                currFR = takeAction(self, prev, curr, currFR);
                prev = curr;
                curr = null;
        });

        self.carrier.on('end', function () {
                takeAction(self, prev, curr, currFR);
                self.emit('end');
        });
}
util.inherits(MpuGarbageCollector, events.EventEmitter);
module.exports = MpuGarbageCollector;

MpuGarbageCollector.prototype.withinGracePeriod =
function withinGracePeriod(date) {
        assert.object(date, 'date');
        assert.ok(date instanceof Date, 'invalid date');

        var self = this;

        return ((self.gcDate - date) > self.gracePeriodMillis);
};


///--- Helpers

/*
 * Emits the 'mpuCleanup' event for the current finalizing record and/or the
 * current object, if they should be garbage collected, and returns the current
 * finalizing record for the current batch of MPU records.
 *
 * A record should be garbage collected if and only if it belongs to a finalized
 * MPU -- that is, a finalizing record is present for the MPU -- and the date of
 * the finalizing record's creation is before the grace period specified for the
 * garbage collector.
 *
 * We can determine whether the current record and the current finalizing record
 * in the stream can be garbage collected as follows. First, we inspect the
 * previous record in the stream. If it has a different upload ID from the
 * current record, then we have started looking at a new MPU, as we know the
 * stream is sorted by upload ID. If a finalizing record for the previous MPU
 * exists, we should emit a cleanup event for it. Next, we look at the current
 * pointer to a finalizing record. If a finalizing record exists for this MPU,
 * then we should emit a cleanup event for it. If the record itself is a
 * finalizing record, then this record becomes the current finalizing record,
 * and is returned from this function.
 *
 * Inputs:
 * - gc: a MpuGarbageCollector object
 * - prev: the previous record in the stream
 * - curr: current record in the stream
 * - currFR: current finalizing record pointer
 *
 */
function takeAction(gc, prev, curr, currFR) {
        assert.optionalObject(prev, 'prev');
        assert.ok(prev instanceof mpuCommon.LiveRecord ||
                  prev instanceof mpuCommon.FinalizingRecord ||
                  !prev);
        assert.optionalObject(curr, 'curr');
        assert.ok(curr instanceof mpuCommon.LiveRecord ||
                  curr instanceof mpuCommon.FinalizingRecord ||
                  !curr);
        assert.optionalObject(currFR, 'currFR');
        assert.ok(currFR instanceof mpuCommon.FinalizingRecord || !currFR);

        if (prev && (!curr || (prev.uploadId !== curr.uploadId))) {
                /*
                 * We've seen all records related to the previous upload ID,
                 * so we know it's safe to delete the finalizing record of the
                 * upload, if the record exists.
                 */
                if (currFR && gc.withinGracePeriod(currFR.date)) {
                        gc.emit('mpuCleanup', currFR);
                        currFR = null;
                }
        }

        if (curr) {
                if (curr instanceof mpuCommon.FinalizingRecord) {
                        currFR = curr;
                } else {
                        /*
                         * Don't garbage collect any records for uploads that
                         * don't have an associated finalizing record.
                         */
                        if (currFR && gc.withinGracePeriod(currFR.date)) {
                                gc.emit('mpuCleanup', curr);
                        }
                }
        }

        return (currFR);
}
