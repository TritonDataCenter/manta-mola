/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var events = require('events');
var path = require('path');
var util = require('util');

var mpuCommon = require('./mpu/common');

var SchemaReader = require('./schema_reader');

///--- GLOBALS

var PG_LIVE_MANTA_TABLE_NAME = 'manta';
var PG_MANTA_UPLOADS_TABLE_NAME = 'manta_uploads';

/* JSSTYLED */
var UPLOADS_ROOT_PATH = /^\/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\/uploads\/?.*/;


///--- API

/*
 * This transforms pg-dumped rows to objects usable for multipart upload garbage
 * collection.
 *
 * The resulting rows are emitted as an object: either a LiveRecord or a
 * FinalizingRecord. LiveRecord objects represent live objects in Manta, which
 * includes parts and upload directories for MPU garbage collection.
 * FinalizingRecord objects map to finalizing records.
 *
 * Both objects expose a toString method for representing the row in a format
 * suitable for sorting.
 *
 * The MpuGcPgRowTransformer is analogous to the GcPgRowTransformer object for
 * normal garbage collection.
 */
function MpuGcPgRowTransformer(opts, listener) {
        assert.object(opts, 'opts');
        assert.object(opts.reader, 'opts.reader');
        assert.ok(opts.dumpDate, 'opts.dumpDate');
        assert.ok(opts.dumpDate instanceof Date, 'invalid date');
        assert.string(opts.morayHostname, 'opts.morayHostname');

        var self = this;
        var reader = opts.reader;
        var dumpDate = opts.dumpDate;
        var earliestDumpDate = opts.earliestDumpDate;
        var morayHostname = opts.morayHostname;

        self.schemaReader = new SchemaReader(reader);

        if (listener) {
                self.addListener('row', listener);
        }

        function isMpuRecord(o) {
                var t = o._value.type;
                var k = o._value.key;
                var u = o._value.upload;

                if (k.match(UPLOADS_ROOT_PATH)) {
                        return (t === 'object' ||
                                (t === 'directory' && !!u));
                }

                return (false);
        }

        self.schemaReader.on('object', function (obj) {
                var table = obj['__table'];
                var row;

                if (table === PG_LIVE_MANTA_TABLE_NAME) {
                        if (isMpuRecord(obj)) {
                            row = transformLiveRecord(obj, dumpDate,
                                morayHostname);
                        }
                } else if (table === PG_MANTA_UPLOADS_TABLE_NAME) {
                        row = transformFinalizingRecord(obj, earliestDumpDate,
                            morayHostname);
                }

                if (row) {
                        self.emit('row', row);
                }
        });

        self.schemaReader.on('end', function () {
                self.emit('end');
        });
}

util.inherits(MpuGcPgRowTransformer, events.EventEmitter);
module.exports = MpuGcPgRowTransformer;


///--- Helpers

function transformFinalizingRecord(obj, dumpDate, morayHostname) {
        assert.string(obj['__table'], PG_MANTA_UPLOADS_TABLE_NAME);
        var value = obj['_value'];
        var date = new Date(parseInt(obj['_mtime'], 10));

        return new mpuCommon.FinalizingRecord({
                uploadId: value.uploadId,
                key: obj._key,
                shard: morayHostname,
                date: date,
                type: value.finalizingType
        });
}

function transformLiveRecord(obj, dumpDate, morayHostname) {
        assert.string(obj['__table'], PG_LIVE_MANTA_TABLE_NAME);
        var value = obj['_value'];

        var mpuObject, uploadId;
        if (value.type === 'directory') {
            mpuObject = mpuCommon.MPU_UPLOADDIR;
            uploadId = value.upload.id;
        } else if (value.type === 'object') {
            mpuObject = mpuCommon.MPU_PART;
            uploadId = path.basename(path.dirname(obj._key));
        } else {
            return (null);
        }
        assert.string(uploadId, 'uploadId');
        assert.string(mpuObject, 'mpuObject');

        if (!obj._key.match(UPLOADS_ROOT_PATH)) {
                return (null);
        }
        assert.string(mpuObject);

        var record = new mpuCommon.LiveRecord({
                key: obj._key,
                date: dumpDate,
                type: mpuObject,
                uploadId: uploadId
        });
        assert.object(record);

        return (record);
}
