/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
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

var MBS_ARGS = {
        log: LOG
};


///--- Helpers

function testMpuBatchStream(args) {
        assert.object(args, 'args');
        assert.arrayOfString(args.input, 'args.input');
        assert.arrayOfObject(args.output, 'args.output');
        assert.func(args.testCb, 'args.testCb');

        var vsArgs = {
                cb: args.testCb,
                expect: args.output
        };

        var r = new stream.Readable({
                objectMode: true
        });
        var mbs = new mpu.createMpuBatchStream(MBS_ARGS);
        var vs = new inputs.ValidationStream(vsArgs);

        args.input.forEach(function (i) {
                r.push(i, 'utf8');
        });
        r.push(null);
        r.pipe(mbs).pipe(vs);
}


///--- Tests

test('single-record batch (FR only)', function (t) {
        var input = [
                inputs.FR_0
        ];

        var output = [ {
                uploadId: inputs.ID_0,
                records: [
                        inputs.OBJ_FR0
                ]
        } ];

        var args = {
                input: input,
                output: output,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        t.done();
                }
        };

        testMpuBatchStream(args);
});

test('multiple-record batch (FR, UR)', function (t) {
        var input = [
                inputs.FR_0, inputs.UR_0
        ];

        var output = [ {
                uploadId: inputs.ID_0,
                records: [
                        inputs.OBJ_FR0, inputs.OBJ_UR0
                ]
        } ];

        var args = {
                input: input,
                output: output,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        t.done();
                }
        };

        testMpuBatchStream(args);
});

test('multiple-record batch (with parts)', function (t) {
        var input = [
                inputs.FR_0, inputs.UR_0, inputs.PR_0[0]
        ];

        var output = [ {
                uploadId: inputs.ID_0,
                records: [
                        inputs.OBJ_FR0, inputs.OBJ_UR0, inputs.OBJ_PR0[0]
                ]
        } ];

        var args = {
                input: input,
                output: output,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        t.done();
                }
        };

        testMpuBatchStream(args);
});

test('multiple batches', function (t) {
        var input = [
                inputs.FR_0, inputs.UR_0, inputs.PR_0[0],
                inputs.FR_2, inputs.UR_2,
                inputs.FR_1, inputs.UR_1, inputs.PR_1[0], inputs.PR_1[1],
                        inputs.PR_1[2]
        ];

        var output = [
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_FR0,
                                inputs.OBJ_UR0,
                                inputs.OBJ_PR0[0]
                        ]
                },
                {
                        uploadId: inputs.ID_2,
                        records: [
                                inputs.OBJ_FR2,
                                inputs.OBJ_UR2
                        ]
                },
                {
                        uploadId: inputs.ID_1,
                        records: [
                                inputs.OBJ_FR1,
                                inputs.OBJ_UR1,
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_PR1[1],
                                inputs.OBJ_PR1[2]
                        ]
                }
        ];

        var args = {
                input: input,
                output: output,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        t.done();
                }
        };

        testMpuBatchStream(args);
});

test('records out of order', function (t) {
        var input = [
                inputs.FR_0, inputs.UR_0,
                inputs.FR_2, inputs.UR_2,
                inputs.PR_0[0]
        ];

        var output = [];

        var args = {
                input: input,
                output: output,
                testCb: function cb(ok, actual) {
                        t.fail('records out of order');
                        if (ok) {
                                console.error('invalid output', actual);
                        }
                        t.done();
                }
        };

        var d = require('domain').create();
        d.on('error', function (err) {
                t.ok(err, 'no error');
                t.done();
        });
        d.run(function () {
                testMpuBatchStream(args);
        });
});
