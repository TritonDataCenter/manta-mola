/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var events = require('events');
var fs = require('fs');
var jsprim = require('jsprim');
var lstream = require('lstream');
var MemoryStream = require('memorystream');
var util = require('util');
var uuid = require('libuuid');
var stream = require('stream');

var helper = require('../helper');
var inputs = require('./testInputs');
var mpu = require('../../lib/mpu');
var mpuCommon = require('../../lib/mpu/common');

///--- Globals

var test = helper.test;
var sprintf = util.format;

var LOG = helper.createLogger('mpuBatchStream test');

var MPU_MORAY_BUCKET = 'manta_uploads';


///--- Helpers

function testMpuMorayCleanerStream(args) {
        assert.object(args, 'args');
        assert.arrayOfObject(args.input, 'args.input');
        assert.arrayOfString(args.shards, 'args.shards');
        assert.ok(args.shards.length > 0, 'at least 1 shard must be specified');
        assert.func(args.delObjectFunc, 'args.delObjectFunc');
        assert.func(args.testCb, 'args.testCb');

        var mockMorayClients = {};
        var clientsClosed = [];

        function MockMorayClient(opts) {
                assert.object(opts, 'opts');
                assert.string(opts.shard, 'opts.shard');

                var self = this;
                self.id = opts.shard;
                self.delObject = opts.delObjectFunc;
        }
        util.inherits(MockMorayClient, events.EventEmitter);
        MockMorayClient.prototype.close = function () {
                clientsClosed.push(this.id);
        };

        args.shards.forEach(function (i) {
                var client = new MockMorayClient({
                        shard: i,
                        delObjectFunc: args.delObjectFunc
                });
                mockMorayClients[i] = {
                        client: client,
                        connected: false
                };

                setImmediate(client.emit.bind(client, 'connect'));
        });

        var mmcs = new mpu.createMpuMorayCleanerStream({
                log: LOG
        });
        mmcs.morayClients = mockMorayClients;

        var r = new stream.Readable({
                objectMode: true
        });

        args.input.forEach(function (i) {
                r.push(i);
        });
        r.push(null);
        r.pipe(mmcs);

        mmcs.on('finish', function () {
                args.testCb(clientsClosed);
        });
}

///--- Tests

test('one batch', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_0,
                        finalizingRecord: inputs.OBJ_FR0,
                        uploadRecord: inputs.OBJ_UR0
                }
        ];

        var shards = [ inputs.SHARD_0 ];
        var keys = [];
        var expected = [
                inputs.KEY_FR0
        ];

        function delObject(bucket, key, dcb) {
                t.ok(bucket === MPU_MORAY_BUCKET);
                t.ok(key === inputs.KEY_FR0);

                keys.push(key);
                dcb();
        }

        var args = {
                input: input,
                testCb: function cb(clientsClosed) {
                        t.ok(jsprim.deepEqual(clientsClosed, shards));
                        t.ok(jsprim.deepEqual(keys, expected));
                        t.done();
                },
                delObjectFunc: delObject,
                shards: shards
        };

        testMpuMorayCleanerStream(args);
});

test('multiple batches', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_0,
                        finalizingRecord: inputs.OBJ_FR0,
                        uploadRecord: inputs.OBJ_UR0,
                        partRecords: [
                                inputs.OBJ_PR0[0]
                        ]
                },
                {
                        uploadId: inputs.ID_2,
                        finalizingRecord: inputs.OBJ_FR2,
                        uploadRecord: inputs.OBJ_UR2
                },
                {
                        uploadId: inputs.ID_1,
                        finalizingRecord: inputs.OBJ_FR1,
                        uploadRecord: inputs.OBJ_UR1,
                        partRecords: [
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_PR1[1],
                                inputs.OBJ_PR1[2]
                        ]
                }
        ];

        var shards = [ inputs.SHARD_0, inputs.SHARD_1, inputs.SHARD_2 ];
        var keys = [];
        var expected = [
                inputs.KEY_FR0,
                inputs.KEY_FR2,
                inputs.KEY_FR1
        ];

        function delObject(bucket, key, dcb) {
                t.ok(bucket === MPU_MORAY_BUCKET);
                t.ok(key === inputs.KEY_FR0 ||
                        key === inputs.KEY_FR1 ||
                        key === inputs.KEY_FR2);

                keys.push(key);
                dcb();
        }

        var args = {
                input: input,
                testCb: function cb(clientsClosed) {
                        shards.forEach(function (s) {
                                t.ok(clientsClosed.indexOf(s) !== -1);
                        });
                        t.ok(jsprim.deepEqual(keys, expected));
                        t.done();
                },
                delObjectFunc: delObject,
                shards: shards
        };

        testMpuMorayCleanerStream(args);
});

test('deleteFinalizingRecord returns error', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_0,
                        finalizingRecord: inputs.OBJ_FR0,
                        uploadRecord: inputs.OBJ_UR0,
                        partRecords: [
                                inputs.OBJ_PR0[0]
                        ]
                },
                {
                        uploadId: inputs.ID_2,
                        finalizingRecord: inputs.OBJ_FR2,
                        uploadRecord: inputs.OBJ_UR2
                },
                {
                        uploadId: inputs.ID_1,
                        finalizingRecord: inputs.OBJ_FR1,
                        uploadRecord: inputs.OBJ_UR1,
                        partRecords: [
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_PR1[1],
                                inputs.OBJ_PR1[2]
                        ]
                }
        ];

        var shards = [ inputs.SHARD_0, inputs.SHARD_1, inputs.SHARD_2 ];
        var keys = [];
        var expected = [
                inputs.KEY_FR0,
                inputs.KEY_FR2,
                inputs.KEY_FR1
        ];

        function delObject(bucket, key, dcb) {
                t.ok(bucket === MPU_MORAY_BUCKET);
                t.ok(key === inputs.KEY_FR0 ||
                        key === inputs.KEY_FR1 ||
                        key === inputs.KEY_FR2);

                keys.push(key);
                var err;
                if (key === inputs.KEY_FR2) {
                        err = new Error('simulated moray error');
                }

                dcb(err);
        }

        var args = {
                input: input,
                testCb: function cb(clientsClosed) {
                        shards.forEach(function (s) {
                                t.ok(clientsClosed.indexOf(s) !== -1);
                        });
                        t.ok(jsprim.deepEqual(keys, expected));
                        t.done();
                },
                delObjectFunc: delObject,
                shards: shards
        };

        testMpuMorayCleanerStream(args);
});
