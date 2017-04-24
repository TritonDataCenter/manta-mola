/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var util = require('util');


///--- Globals

var sprintf = util.format;

var MPU_MORAY_BUCKET = 'manta_uploads';

/*
 * Internal constants used to differentiate between parts and upload records, as
 * they are often used in the same context.
 */
var MPU_PART = 'partRecord';
var MPU_UPLOADDIR = 'uploadRecord';

/*
 * MPU object values used by the record transformation step of the GC job
 * (bin/mpu_gc_pg_transform.js). They are prepended with a numeral to ensure
 * they sort in a given order: namely, that the finalizing record will be listed
 * first in a sorted list.
 */
var MPUOBJ_PART = '2_partRecord';
var MPUOBJ_UPLOADDIR = '1_uploadRecord';
var MPUOBJ_FINALIZINGRECORD = '0_finalizingRecord';

/*
 * Types of finalizing records. The type doesn't make a difference with regard
 * to garbage collection, but it is good to have these recorded in the state of
 * MPUs for debugging purposes.
 */
var MPU_FR_TYPE_COMMIT = 'commit';
var MPU_FR_TYPE_ABORT = 'abort';


/*
 * Given a string in a known format (namely, the same one produced by the
 * toString method on a FinalizingRecord or LiveRecord object), returns a
 * FinalizingRecord or LiveRecord object with the fields from the string.
 *
 * This function expects the string to match the specified format and will fail
 * assertions if it does not. The responsibility is on the caller to ensure that
 * the string is the correct format.
 *
 * In particular, the format for a finalizing record is a tab-separated list of
 * the following fields:
 *      [upload id]
 *      0_finalizingRecord
 *      [DATE]
 *      [SHARD]
 *      {commit,abort}
 *      [manta_uploads KEY]
 *
 * In particular, the format for a live record is a tab-separated list of the
 * following fields:
 *      [upload id]
 *      {1_uploadRecord, 2_partRecord}
 *      [DATE]
 *      [manta KEY]
 */
function recordToObject(record) {
        assert.string(record);

        var split = record.split('\t');
        assert.ok(split.length >= 4, sprintf('record must contain at least 4 ' +
                'tab-separated fields: \"%s\"', record));

        var uploadId = split[0];
        var mpuObject = split[1];
        var date = new Date(split[2]);
        var key;

        assert.object(date, sprintf('unable to parse date: %s', split[2]));
        assert.ok(date instanceof Date, 'invalid date');
        assert.ok(mpuObject === MPUOBJ_PART ||
                mpuObject === MPUOBJ_UPLOADDIR ||
                mpuObject === MPUOBJ_FINALIZINGRECORD,
                sprintf('invalid mpu object type: \"%s\"', mpuObject));

        if (mpuObject === MPUOBJ_FINALIZINGRECORD) {
                assert.ok(split.length === 6, sprintf('finalizing record ' +
                        'must contain 6 tab-separated fields: \"%s\"', record));

                var shard = split[3];
                var finalizingType = split[4];
                key = split[5];

                assert.ok(finalizingType === MPU_FR_TYPE_COMMIT ||
                          finalizingType === MPU_FR_TYPE_ABORT,
                          sprintf('invalid finalizing type: %s',
                                finalizingType));

                return new FinalizingRecord({
                        uploadId: uploadId,
                        key: key,
                        shard: shard,
                        date: date,
                        type: finalizingType
                });
        } else {
                assert.ok(split.length === 4, 'upload/part records must ' +
                        'contain 4 tab-separated fields');

                key = split[3];

                var mulrsType;
                if (mpuObject === MPUOBJ_UPLOADDIR) {
                        mulrsType = MPU_UPLOADDIR;
                } else {
                        mulrsType = MPU_PART;
                }

                return new LiveRecord({
                        uploadId: uploadId,
                        key: key,
                        type: mulrsType,
                        date: date

                });
        }
}

/*
 * Represents a finalizing record in the streams that process the metadata
 * record cleanup.
 *
 * Parameters:
 * - opts: an object with the following required properties:
 *   - "uploadId": the MPU upload ID
 *   - "key": the Moray key for this record
 *   - "shard": Moray shard of the record
 *   - "date": date on the Moray record
 *   - "type": finalizing type of the MPU
 */
function FinalizingRecord(opts) {
        assert.string(opts.uploadId, 'opts.uploadId');
        assert.string(opts.key, 'opts.key');
        assert.string(opts.shard, 'opts.shard');
        assert.object(opts.date, 'opts.date');
        assert.ok(opts.date instanceof Date, 'invalid date');
        assert.string(opts.type, 'opts.type');

        this.uploadId = opts.uploadId;
        this.key = opts.key;
        this.shard = opts.shard;
        this.date = opts.date;
        this.type = opts.type;
}

FinalizingRecord.prototype.toString = function frToString() {
        return (this.uploadId + '\t' +
                MPUOBJ_FINALIZINGRECORD + '\t' +
                this.date.toISOString() + '\t' +
                this.shard + '\t' +
                this.type + '\t' +
                this.key);
};

/*
 * Represents a "live" Manta record in the streams that process the metadata
 * record cleanup. In particular, a live record is either the part record or
 * upload record of a given MPU.
 *
 * Parameters:
 * - opts: an object with the following required properties:
 *   - "uploadId": the MPU upload ID
 *   - "key": key to record in Moray
 *   - "date": date on the Moray record
 *   - "type": either "uploadRecord" or "partRecord"
 *
 */
function LiveRecord(opts) {
        assert.string(opts.uploadId, 'opts.uploadId');
        assert.string(opts.key, 'opts.key');
        assert.object(opts.date, 'opts.date');
        assert.ok(opts.date instanceof Date, 'invalid date');
        assert.string(opts.type, 'opts.type');
        assert.ok(opts.type === MPU_PART ||
                  opts.type === MPU_UPLOADDIR,
                  sprintf('invalid type: %s', opts.type));

        this.uploadId = opts.uploadId;
        this.key = opts.key;
        this.date = opts.date;
        this.type = opts.type;
}

LiveRecord.prototype.toString = function lrToString() {
        var mpuObj;
        if (this.type === MPU_PART) {
                mpuObj = MPUOBJ_PART;
        } else {
                mpuObj = MPUOBJ_UPLOADDIR;
        }

        return (this.uploadId + '\t' +
                mpuObj + '\t' +
                this.date.toISOString() + '\t' +
                this.key);
};



module.exports = {
        recordToObject: recordToObject,
        LiveRecord: LiveRecord,
        FinalizingRecord: FinalizingRecord,

        MPU_PART: MPU_PART,
        MPU_UPLOADDIR: MPU_UPLOADDIR,

        MPUOBJ_PART: MPUOBJ_PART,
        MPUOBJ_UPLOADDIR: MPUOBJ_UPLOADDIR,
        MPUOBJ_FINALIZINGRECORD: MPUOBJ_FINALIZINGRECORD,

        MPU_MORAY_BUCKET: MPU_MORAY_BUCKET
};
