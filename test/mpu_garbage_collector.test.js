/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var util = require('util');
var vstream = require('vstream');

var helper = require('./helper.js');
var lib = require('../lib');
var mpuCommon = require('../lib/mpu/common');
var MemoryStream = require('memorystream');


///--- Globals

var DEF_GRACE_PERIOD_MILLIS = 60 * 60 * 24 * 2 * 1000; // 2 days
var MORAY_1 = '1.moray.coal.joyent.us';
var MORAY_2 = '2.moray.coal.joyent.us';

var OWNER_0 = libuuid.create();
var OWNER_1 = libuuid.create();
var ID_0 = libuuid.create();
var ID_1 = libuuid.create();
var ID_2 = libuuid.create();
var ID_3 = libuuid.create();

var DATE_GC = new Date('2017-08-30T00:00:00');
var DATE_OUTSIDE_GP = new Date('2017-08-27T00:00:00');
var DATE_WITHIN_GP = new Date('2017-08-29T00:00:00');

var LOG = bunyan.createLogger({
        level: process.env.LOG_LEVEL || 'info',
        name: 'mpu_garbage_collector_test',
        stream: process.stdout,
        serializers: bunyan.stdSerializers
});

var test = helper.test;


///--- Helpers

function uploadRecord(id, date, key) {
        assert.uuid(id, 'id');
        assert.string(date, 'date');
        assert.string(key, 'key');

        return (id + '\t1_uploadRecord\t' + date + '\t' + key);
}

function partRecord(id, date, key) {
        assert.uuid(id, 'id');
        assert.string(date, 'date');
        assert.string(key, 'key');

        return (id + '\t2_partRecord\t' + date + '\t' + key);
}

function commitRecord(id, date, owner, shard) {
        assert.uuid(id, 'id');
        assert.string(date, 'date');
        assert.string(owner, 'owner');
        assert.string(shard, 'shard');

        var key = finalizingRecordKey(id, owner);

        return (id + '\t0_finalizingRecord\t' + date + '\t' + shard +
                '\tcommit\t' + key);
}

function abortRecord(id, date, key, shard) {
        assert.uuid(id, 'id');
        assert.string(date, 'date');
        assert.string(key, 'key');
        assert.string(shard, 'shard');

        return (id + '\t0_finalizingRecord\t' + date + '\t' + shard +
                '\tabort\t' + key);
}

function checkMoray(moray, morayHostname, objectId, date) {
        assert.equal(moray.morayHostname, morayHostname);
        assert.equal(moray.objectId, objectId);
        assert.equal(moray.date - 0, date - 0);
}

function partRecordKey(id, owner, partNum) {
        assert.uuid(id, 'id');
        assert.uuid(owner, 'owner');
        assert.number(partNum, 'partNum');
        assert.ok(partNum >= 0 && partNum < 10000, 'invalid partNum');

        return ('/' + owner + '/uploads/' + id.substring(0, 2) + '/' + id +
                '/' + partNum);
}

function uploadRecordKey(id, owner) {
        assert.uuid(id, 'id');
        assert.uuid(owner, 'owner');

        return ('/' + owner + '/uploads/' + id.substring(0, 2) + '/' + id);
}

function finalizingRecordKey(id, owner) {
        assert.uuid(id, 'id');
        assert.uuid(owner, 'owner');

        return (id + ':' + uploadRecordKey(id, owner));
}

///--- Tests: all within grace period

test('single batch: finalizing record only', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // finalized
                commitRecord(ID_0, date, OWNER_0, MORAY_1),
                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];
        expect.push(mpuCommon.recordToObject(inputs[0]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('single batch: upload record only', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // not finalized
                uploadRecord(ID_0, date, uploadRecordKey(ID_0, OWNER_0)),
                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('single batch: part record only', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // not finalized
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 0)),
                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('single batch: upload and part records', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // not finalized
                uploadRecord(ID_0, date, uploadRecordKey(ID_0, OWNER_0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 1)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 2)),
                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('single batch: finalizing record and upload record', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // finalized
                commitRecord(ID_0, date, OWNER_0, MORAY_1),
                uploadRecord(ID_0, date, uploadRecordKey(ID_0, OWNER_0)),
                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];
        expect.push(mpuCommon.recordToObject(inputs[1]));
        expect.push(mpuCommon.recordToObject(inputs[0]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('single batch: finalizing record, upload record, part records',
function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // finalized
                abortRecord(ID_0, date, OWNER_0, MORAY_1),
                uploadRecord(ID_0, date, uploadRecordKey(ID_0, OWNER_0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 1)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 2)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        expect.push(mpuCommon.recordToObject(inputs[1]));
        expect.push(mpuCommon.recordToObject(inputs[2]));
        expect.push(mpuCommon.recordToObject(inputs[3]));
        expect.push(mpuCommon.recordToObject(inputs[4]));
        expect.push(mpuCommon.recordToObject(inputs[0]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('finalizing records only', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // finalized
                commitRecord(ID_0, date, OWNER_0, MORAY_1),

                // finalized
                commitRecord(ID_1, date, OWNER_0, MORAY_2),

                // finalized
                abortRecord(ID_2, date, OWNER_1, MORAY_1),

                // finalized
                abortRecord(ID_3, date, OWNER_1, MORAY_2),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];
        inputs.forEach(function (r) {
                if (r !== '') {
                        expect.push(mpuCommon.recordToObject(r));
                }
        });

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('all upload records', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // not finalized
                uploadRecord(ID_0, date, uploadRecordKey(ID_0, OWNER_0)),

                // not finalized
                uploadRecord(ID_1, date, uploadRecordKey(ID_1, OWNER_1)),

                // not finalized
                uploadRecord(ID_2, date, uploadRecordKey(ID_2, OWNER_1)),

                // not finalized
                uploadRecord(ID_3, date, uploadRecordKey(ID_3, OWNER_0)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];
        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('all part records', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // not finalized
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 1)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 2)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 3)),

                // not finalized
                partRecord(ID_1, date, partRecordKey(ID_1, OWNER_1, 0)),

                // not finalized
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 0)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 2)),

                // not finalized
                partRecord(ID_3, date, partRecordKey(ID_3, OWNER_0, 0)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];
        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('no finalizing records', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // not finalized
                uploadRecord(ID_0, date, uploadRecordKey(ID_0, OWNER_0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 1)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 2)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 3)),

                // not finalized
                uploadRecord(ID_1, date, uploadRecordKey(ID_1, OWNER_1)),
                partRecord(ID_1, date, partRecordKey(ID_1, OWNER_1, 0)),

                // not finalized
                uploadRecord(ID_2, date, uploadRecordKey(ID_2, OWNER_1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 0)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 2)),

                // not finalized
                uploadRecord(ID_3, date, uploadRecordKey(ID_3, OWNER_0)),
                partRecord(ID_3, date, partRecordKey(ID_3, OWNER_0, 0)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];
        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('all finalized', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // finalized
                commitRecord(ID_0, date, OWNER_0, MORAY_1),
                uploadRecord(ID_0, date, uploadRecordKey(ID_0, OWNER_0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 1)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 2)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 3)),

                // finalized
                commitRecord(ID_1, date, OWNER_1, MORAY_2),
                uploadRecord(ID_1, date, uploadRecordKey(ID_1, OWNER_1)),

                // finalized
                abortRecord(ID_2, date, OWNER_1, MORAY_2),
                uploadRecord(ID_2, date, uploadRecordKey(ID_2, OWNER_1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 0)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 2)),

                // finalized
                abortRecord(ID_3, date, OWNER_0, MORAY_2),
                uploadRecord(ID_3, date, uploadRecordKey(ID_3, OWNER_0)),
                partRecord(ID_3, date, partRecordKey(ID_3, OWNER_0, 0)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        expect.push(mpuCommon.recordToObject(inputs[1]));
        expect.push(mpuCommon.recordToObject(inputs[2]));
        expect.push(mpuCommon.recordToObject(inputs[3]));
        expect.push(mpuCommon.recordToObject(inputs[4]));
        expect.push(mpuCommon.recordToObject(inputs[5]));
        expect.push(mpuCommon.recordToObject(inputs[0]));

        expect.push(mpuCommon.recordToObject(inputs[7]));
        expect.push(mpuCommon.recordToObject(inputs[6]));

        expect.push(mpuCommon.recordToObject(inputs[9]));
        expect.push(mpuCommon.recordToObject(inputs[10]));
        expect.push(mpuCommon.recordToObject(inputs[11]));
        expect.push(mpuCommon.recordToObject(inputs[12]));
        expect.push(mpuCommon.recordToObject(inputs[8]));

        expect.push(mpuCommon.recordToObject(inputs[14]));
        expect.push(mpuCommon.recordToObject(inputs[15]));
        expect.push(mpuCommon.recordToObject(inputs[13]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('finalizing record only batch at beginning', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // finalized
                commitRecord(ID_0, date, OWNER_0, MORAY_1),

                // not finalized
                uploadRecord(ID_1, date, uploadRecordKey(ID_1, OWNER_1)),

                // not finalized
                uploadRecord(ID_2, date, uploadRecordKey(ID_2, OWNER_1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 0)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 2)),

                // not finalized
                uploadRecord(ID_3, date, uploadRecordKey(ID_3, OWNER_0)),
                partRecord(ID_3, date, partRecordKey(ID_3, OWNER_0, 0)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];
        expect.push(mpuCommon.recordToObject(inputs[0]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('finalizing record only batch at end', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // not finalized
                uploadRecord(ID_1, date, uploadRecordKey(ID_1, OWNER_1)),

                // not finalized
                uploadRecord(ID_2, date, uploadRecordKey(ID_2, OWNER_1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 0)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 2)),

                // not finalized
                uploadRecord(ID_3, date, uploadRecordKey(ID_3, OWNER_0)),
                partRecord(ID_3, date, partRecordKey(ID_3, OWNER_0, 0)),

                // finalized
                commitRecord(ID_0, date, OWNER_0, MORAY_1),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];
        expect.push(mpuCommon.recordToObject(inputs[7]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('upload record only batch at beginning', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // not finalized
                uploadRecord(ID_0, date, uploadRecordKey(ID_0, OWNER_0)),

                // finalized
                commitRecord(ID_1, date, OWNER_1, MORAY_2),
                uploadRecord(ID_1, date, uploadRecordKey(ID_1, OWNER_1)),

                // finalized
                abortRecord(ID_2, date, OWNER_1, MORAY_2),
                uploadRecord(ID_2, date, uploadRecordKey(ID_2, OWNER_1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 0)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 2)),

                // finalized
                abortRecord(ID_3, date, OWNER_0, MORAY_2),
                uploadRecord(ID_3, date, uploadRecordKey(ID_3, OWNER_0)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        expect.push(mpuCommon.recordToObject(inputs[2]));
        expect.push(mpuCommon.recordToObject(inputs[1]));

        expect.push(mpuCommon.recordToObject(inputs[4]));
        expect.push(mpuCommon.recordToObject(inputs[5]));
        expect.push(mpuCommon.recordToObject(inputs[6]));
        expect.push(mpuCommon.recordToObject(inputs[7]));
        expect.push(mpuCommon.recordToObject(inputs[3]));

        expect.push(mpuCommon.recordToObject(inputs[9]));
        expect.push(mpuCommon.recordToObject(inputs[8]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('upload record only batch at end', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // finalized
                commitRecord(ID_0, date, OWNER_0, MORAY_2),
                uploadRecord(ID_0, date, uploadRecordKey(ID_0, OWNER_0)),

                // finalized
                commitRecord(ID_1, date, OWNER_1, MORAY_2),
                uploadRecord(ID_1, date, uploadRecordKey(ID_1, OWNER_1)),

                // finalized
                abortRecord(ID_2, date, OWNER_1, MORAY_2),
                uploadRecord(ID_2, date, uploadRecordKey(ID_2, OWNER_1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 0)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 2)),

                // not finalized
                uploadRecord(ID_3, date, uploadRecordKey(ID_3, OWNER_0)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        expect.push(mpuCommon.recordToObject(inputs[1]));
        expect.push(mpuCommon.recordToObject(inputs[0]));

        expect.push(mpuCommon.recordToObject(inputs[3]));
        expect.push(mpuCommon.recordToObject(inputs[2]));

        expect.push(mpuCommon.recordToObject(inputs[5]));
        expect.push(mpuCommon.recordToObject(inputs[6]));
        expect.push(mpuCommon.recordToObject(inputs[7]));
        expect.push(mpuCommon.recordToObject(inputs[8]));
        expect.push(mpuCommon.recordToObject(inputs[4]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('part record only batch at beginning', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // not finalized
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 0)),

                // finalized
                commitRecord(ID_1, date, OWNER_1, MORAY_2),
                uploadRecord(ID_1, date, uploadRecordKey(ID_1, OWNER_1)),

                // finalized
                abortRecord(ID_2, date, OWNER_1, MORAY_2),
                uploadRecord(ID_2, date, uploadRecordKey(ID_2, OWNER_1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 0)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 2)),

                // finalized
                commitRecord(ID_3, date, OWNER_0, MORAY_2),
                uploadRecord(ID_3, date, uploadRecordKey(ID_3, OWNER_0)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        expect.push(mpuCommon.recordToObject(inputs[2]));
        expect.push(mpuCommon.recordToObject(inputs[1]));

        expect.push(mpuCommon.recordToObject(inputs[4]));
        expect.push(mpuCommon.recordToObject(inputs[5]));
        expect.push(mpuCommon.recordToObject(inputs[6]));
        expect.push(mpuCommon.recordToObject(inputs[7]));
        expect.push(mpuCommon.recordToObject(inputs[3]));

        expect.push(mpuCommon.recordToObject(inputs[9]));
        expect.push(mpuCommon.recordToObject(inputs[8]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });

});

test('part record at end', function (t) {
        var date = DATE_OUTSIDE_GP.toISOString();

        var inputs = [
                // finalized
                commitRecord(ID_1, date, OWNER_1, MORAY_2),
                uploadRecord(ID_1, date, uploadRecordKey(ID_1, OWNER_1)),

                // finalized
                abortRecord(ID_2, date, OWNER_1, MORAY_2),
                uploadRecord(ID_2, date, uploadRecordKey(ID_2, OWNER_1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 0)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 1)),
                partRecord(ID_2, date, partRecordKey(ID_2, OWNER_1, 2)),

                // finalized
                commitRecord(ID_3, date, OWNER_0, MORAY_2),
                uploadRecord(ID_3, date, uploadRecordKey(ID_3, OWNER_0)),

                // not finalized
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 0)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        expect.push(mpuCommon.recordToObject(inputs[1]));
        expect.push(mpuCommon.recordToObject(inputs[0]));

        expect.push(mpuCommon.recordToObject(inputs[3]));
        expect.push(mpuCommon.recordToObject(inputs[4]));
        expect.push(mpuCommon.recordToObject(inputs[5]));
        expect.push(mpuCommon.recordToObject(inputs[6]));
        expect.push(mpuCommon.recordToObject(inputs[2]));

        expect.push(mpuCommon.recordToObject(inputs[8]));
        expect.push(mpuCommon.recordToObject(inputs[7]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});


///--- Tests: testing grace period

test('single batch: finalizing record only, within grace period',
function (t) {
        var date = DATE_WITHIN_GP.toISOString();

        var inputs = [
                // finalized, within grace period
                commitRecord(ID_0, date, OWNER_0, MORAY_1),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('single batch: multiple records, within grace period', function (t) {
        var date = DATE_WITHIN_GP.toISOString();

        var inputs = [
                // finalized, within grace period
                abortRecord(ID_0, date, OWNER_0, MORAY_1),
                uploadRecord(ID_0, date, uploadRecordKey(ID_0, OWNER_0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 1)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 2)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('single batch: finalized, barely within grace period',
function (t) {
        var ms = DATE_GC.valueOf() - DEF_GRACE_PERIOD_MILLIS + 1000;
        var date = new Date(ms).toISOString();

        var inputs = [
                // finalized, within grace period
                abortRecord(ID_0, date, OWNER_0, MORAY_1),
                uploadRecord(ID_0, date, uploadRecordKey(ID_0, OWNER_0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 1)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 2)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('single batch: finalized, barely outside grace period',
function (t) {
        var ms = DATE_GC.valueOf() - DEF_GRACE_PERIOD_MILLIS - 1000;
        var date = new Date(ms).toISOString();

        var inputs = [
                // finalized, outside grace period
                abortRecord(ID_0, date, OWNER_0, MORAY_1),
                uploadRecord(ID_0, date, uploadRecordKey(ID_0, OWNER_0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 0)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 1)),
                partRecord(ID_0, date, partRecordKey(ID_0, OWNER_0, 2)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];
        expect.push(mpuCommon.recordToObject(inputs[1]));
        expect.push(mpuCommon.recordToObject(inputs[2]));
        expect.push(mpuCommon.recordToObject(inputs[3]));
        expect.push(mpuCommon.recordToObject(inputs[4]));
        expect.push(mpuCommon.recordToObject(inputs[0]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('all finalized, some within grace period', function (t) {
        var outsideGpMs = DATE_GC.valueOf() - DEF_GRACE_PERIOD_MILLIS - 1000;
        var outsideGp = new Date(outsideGpMs).toISOString();

        var withinGpMs = DATE_GC.valueOf() - DEF_GRACE_PERIOD_MILLIS + 1000;
        var withinGp = new Date(withinGpMs).toISOString();

        var inputs = [
                // finalized, within grace period
                commitRecord(ID_0, withinGp, OWNER_0, MORAY_1),
                uploadRecord(ID_0, withinGp, uploadRecordKey(ID_0, OWNER_0)),
                partRecord(ID_0, withinGp, partRecordKey(ID_0, OWNER_0, 0)),
                partRecord(ID_0, withinGp, partRecordKey(ID_0, OWNER_0, 1)),
                partRecord(ID_0, withinGp, partRecordKey(ID_0, OWNER_0, 2)),
                partRecord(ID_0, withinGp, partRecordKey(ID_0, OWNER_0, 3)),

                // finalized, outside grace period
                commitRecord(ID_1, outsideGp, OWNER_1, MORAY_2),
                uploadRecord(ID_1, outsideGp, uploadRecordKey(ID_1, OWNER_1)),

                // finalized, within grace period
                abortRecord(ID_2, withinGp, OWNER_1, MORAY_2),
                uploadRecord(ID_2, withinGp, uploadRecordKey(ID_2, OWNER_1)),
                partRecord(ID_2, withinGp, partRecordKey(ID_2, OWNER_1, 0)),
                partRecord(ID_2, withinGp, partRecordKey(ID_2, OWNER_1, 1)),
                partRecord(ID_2, withinGp, partRecordKey(ID_2, OWNER_1, 2)),

                // finalized, outside grace period
                abortRecord(ID_3, outsideGp, OWNER_0, MORAY_2),
                uploadRecord(ID_3, outsideGp, uploadRecordKey(ID_3, OWNER_0)),
                partRecord(ID_3, outsideGp, partRecordKey(ID_3, OWNER_0, 0)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG
        });
        var expect = [];

        expect.push(mpuCommon.recordToObject(inputs[7]));
        expect.push(mpuCommon.recordToObject(inputs[6]));

        expect.push(mpuCommon.recordToObject(inputs[14]));
        expect.push(mpuCommon.recordToObject(inputs[15]));
        expect.push(mpuCommon.recordToObject(inputs[13]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('all finalized, some within custom grace period', function (t) {
        var gracePeriod = 1000;

        var outsideGpMs = DATE_GC.valueOf() - gracePeriod - 1000;
        var outsideGp = new Date(outsideGpMs).toISOString();

        var withinGpMs = DATE_GC.valueOf() - gracePeriod + 1000;
        var withinGp = new Date(withinGpMs).toISOString();

        var inputs = [
                // finalized, within grace period
                commitRecord(ID_0, withinGp, OWNER_0, MORAY_1),
                uploadRecord(ID_0, withinGp, uploadRecordKey(ID_0, OWNER_0)),
                partRecord(ID_0, withinGp, partRecordKey(ID_0, OWNER_0, 0)),
                partRecord(ID_0, withinGp, partRecordKey(ID_0, OWNER_0, 1)),
                partRecord(ID_0, withinGp, partRecordKey(ID_0, OWNER_0, 2)),
                partRecord(ID_0, withinGp, partRecordKey(ID_0, OWNER_0, 3)),

                // finalized, outside grace period
                commitRecord(ID_1, outsideGp, OWNER_1, MORAY_2),
                uploadRecord(ID_1, outsideGp, uploadRecordKey(ID_1, OWNER_1)),

                // finalized, within grace period
                abortRecord(ID_2, withinGp, OWNER_1, MORAY_2),
                uploadRecord(ID_2, withinGp, uploadRecordKey(ID_2, OWNER_1)),
                partRecord(ID_2, withinGp, partRecordKey(ID_2, OWNER_1, 0)),
                partRecord(ID_2, withinGp, partRecordKey(ID_2, OWNER_1, 1)),
                partRecord(ID_2, withinGp, partRecordKey(ID_2, OWNER_1, 2)),

                // finalized, outside grace period
                abortRecord(ID_3, outsideGp, OWNER_0, MORAY_2),
                uploadRecord(ID_3, outsideGp, uploadRecordKey(ID_3, OWNER_0)),
                partRecord(ID_3, outsideGp, partRecordKey(ID_3, OWNER_0, 0)),

                ''
        ];

        var stream = new MemoryStream(inputs.join('\n'));
        var gc = lib.createMpuGarbageCollector({
                reader: stream,
                gcDate: DATE_GC,
                log: LOG,
                gracePeriodMillis: gracePeriod
        });
        var expect = [];

        expect.push(mpuCommon.recordToObject(inputs[7]));
        expect.push(mpuCommon.recordToObject(inputs[6]));

        expect.push(mpuCommon.recordToObject(inputs[14]));
        expect.push(mpuCommon.recordToObject(inputs[15]));
        expect.push(mpuCommon.recordToObject(inputs[13]));

        var output = [];

        gc.on('mpuCleanup', function (action) {
                output.push(action);
        });

        gc.on('end', function () {
                t.ok(jsprim.deepEqual(expect, output));
                t.done();
        });

        process.nextTick(function () {
                stream.end();
        });
});
