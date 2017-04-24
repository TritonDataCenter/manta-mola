/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */


///--- Globals

var assert = require('assert-plus');
var stream = require('stream');

var helper = require('../helper');
var inputs = require('./testInputs');
var mpu = require('../../lib/mpu');
var test = helper.test;

var LOG = helper.createLogger('mpuVerifyStream test');

var MVS_ARGS = {
        log: LOG
};

///--- Helpers

function testMpuVerifyStream(args) {
        assert.object(args, 'args');
        assert.arrayOfObject(args.input, 'args.input');
        assert.arrayOfObject(args.output, 'args.output');
        assert.func(args.testCb, 'args.testCb');

        var vsArgs = {
                cb: args.testCb,
                expect: args.output
        };

        var mvs = new mpu.createMpuVerifyStream(MVS_ARGS);
        var vs = new inputs.ValidationStream(vsArgs);
        var r = new stream.Readable({
                objectMode: true
        });

        args.input.forEach(function (i) {
                r.push(i);
        });
        r.push(null);
        r.pipe(mvs).pipe(vs);
}

///--- Tests

test('single-record batch (FR only, commit)', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_FR0
                        ]
                }
        ];

        var output = [
                {
                        uploadId: inputs.ID_0,
                        finalizingRecord: inputs.OBJ_FR0,
                        uploadRecord: undefined,
                        partRecords: undefined
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

        testMpuVerifyStream(args);
});

test('single-record batch (FR only, abort)', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_1,
                        records: [
                                inputs.OBJ_FR1
                        ]
                }
        ];

        var output = [
                {
                        uploadId: inputs.ID_1,
                        finalizingRecord: inputs.OBJ_FR1,
                        uploadRecord: undefined,
                        partRecords: undefined
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

        testMpuVerifyStream(args);
});



test('multiple-record batch (FR, UR)', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_FR0,
                                inputs.OBJ_UR0
                        ]
                }
        ];

        var output = [
                {
                        uploadId: inputs.ID_0,
                        finalizingRecord: inputs.OBJ_FR0,
                        uploadRecord: inputs.OBJ_UR0,
                        partRecords: undefined
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

        testMpuVerifyStream(args);
});

test('multiple-record batch (1 part)', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_FR0,
                                inputs.OBJ_UR0,
                                inputs.OBJ_PR0[0]
                        ]
                }
        ];

        var output = [
                {
                        uploadId: inputs.ID_0,
                        finalizingRecord: inputs.OBJ_FR0,
                        uploadRecord: inputs.OBJ_UR0,
                        partRecords: [
                                inputs.OBJ_PR0[0]
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

        testMpuVerifyStream(args);
});

test('multiple-record batch (3 parts)', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_1,
                        records: [
                                inputs.OBJ_FR1,
                                inputs.OBJ_UR1,
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_PR1[1]
                        ]
                }
        ];

        var output = [
                {
                        uploadId: inputs.ID_1,
                        finalizingRecord: inputs.OBJ_FR1,
                        uploadRecord: inputs.OBJ_UR1,
                        partRecords: [
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_PR1[1]
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

        testMpuVerifyStream(args);
});

test('multiple batches', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_1,
                        records: [
                                inputs.OBJ_FR1,
                                inputs.OBJ_UR1,
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_PR1[1]
                        ]
                },
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
                }
        ];

        var output = [
                {
                        uploadId: inputs.ID_1,
                        finalizingRecord: inputs.OBJ_FR1,
                        uploadRecord: inputs.OBJ_UR1,
                        partRecords: [
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_PR1[1]
                        ]
                },
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
                        uploadRecord: inputs.OBJ_UR2,
                        partRecords: undefined
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

        testMpuVerifyStream(args);
});

// Bad Input: We expect batches with bad input to be dropped from the stream.

test('batch with different upload ids between FR and UR', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_FR0,
                                inputs.OBJ_UR1
                        ]
                }
        ];

        var output = [];

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

        testMpuVerifyStream(args);
});

test('batch with different upload ids between FR and PR', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_FR0,
                                inputs.OBJ_PR1[0]
                        ]
                }
        ];

        var output = [];

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

        testMpuVerifyStream(args);
});


test('batch with different upload ids between UR and PR', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_UR0,
                                inputs.OBJ_PR1[0]
                        ]
                }
        ];

        var output = [];

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

        testMpuVerifyStream(args);
});


test('batch with different upload ids: multiple batches', function (t) {
        var input = [
                // valid batch
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_FR0,
                                inputs.OBJ_UR0,
                                inputs.OBJ_PR0[0]
                        ]
                },
                // invalid batch
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_UR0,
                                inputs.OBJ_PR1[0]
                        ]
                },
                // valid batch
                {
                        uploadId: inputs.ID_2,
                        records: [
                                inputs.OBJ_FR2,
                                inputs.OBJ_UR2
                        ]
                }
        ];

        var output = [
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
                        uploadRecord: inputs.OBJ_UR2,
                        partRecords: undefined
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

        testMpuVerifyStream(args);
});

test('missing FR', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_1,
                        records: [
                                inputs.OBJ_UR1
                        ]
                }
        ];

        var output = [];

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

        testMpuVerifyStream(args);
});

test('missing FR: multiple batches', function (t) {
        var input = [
                // valid batch
                {
                        uploadId: inputs.ID_1,
                        records: [
                                inputs.OBJ_FR1,
                                inputs.OBJ_UR1,
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_PR1[1],
                                inputs.OBJ_PR1[2]
                        ]
                },
                // invalid batch
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_UR0,
                                inputs.OBJ_PR0[0]
                        ]
                },
                // valid batch
                {
                        uploadId: inputs.ID_2,
                        records: [
                                inputs.OBJ_FR2,
                                inputs.OBJ_UR2
                        ]
                }
        ];

        var output = [
                {
                        uploadId: inputs.ID_1,
                        finalizingRecord: inputs.OBJ_FR1,
                        uploadRecord: inputs.OBJ_UR1,
                        partRecords: [
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_PR1[1],
                                inputs.OBJ_PR1[2]
                        ]
                },
                {
                        uploadId: inputs.ID_2,
                        finalizingRecord: inputs.OBJ_FR2,
                        uploadRecord: inputs.OBJ_UR2,
                        partRecords: undefined
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

        testMpuVerifyStream(args);
});

test('multiple FR: one batch', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_1,
                        records: [
                                inputs.OBJ_FR1,
                                inputs.OBJ_UR1,
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_FR1
                        ]
                }
        ];

        var output = [];

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

        testMpuVerifyStream(args);
});

test('multiple FR: multiple batches', function (t) {
        var input = [
                // invalid batch
                {
                        uploadId: inputs.ID_1,
                        records: [
                                inputs.OBJ_FR1,
                                inputs.OBJ_UR1,
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_FR1,
                                inputs.OBJ_PR1[1],
                                inputs.OBJ_PR1[2]
                        ]
                },
                // valid batch
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_FR0,
                                inputs.OBJ_UR0,
                                inputs.OBJ_PR0[0]
                        ]
                },
                // valid batch
                {
                        uploadId: inputs.ID_2,
                        records: [
                                inputs.OBJ_FR2,
                                inputs.OBJ_UR2
                        ]
                }
        ];

        var output = [
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
                        uploadRecord: inputs.OBJ_UR2,
                        partRecords: undefined
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

        testMpuVerifyStream(args);
});

test('multiple UR: one batch', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_1,
                        records: [
                                inputs.OBJ_FR1,
                                inputs.OBJ_UR1,
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_UR1
                        ]
                }
        ];

        var output = [];

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

        testMpuVerifyStream(args);
});

test('multiple FR: multiple batches', function (t) {
        var input = [
                // valid batch
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_FR0,
                                inputs.OBJ_UR0,
                                inputs.OBJ_PR0[0]
                        ]
                },
                // valid batch
                {
                        uploadId: inputs.ID_2,
                        records: [
                                inputs.OBJ_FR2,
                                inputs.OBJ_UR2
                        ]
                },
                // invalid batch
                {
                        uploadId: inputs.ID_1,
                        records: [
                                inputs.OBJ_FR1,
                                inputs.OBJ_UR1,
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_UR1,
                                inputs.OBJ_PR1[1],
                                inputs.OBJ_PR1[2]
                        ]
                }
        ];

        var output = [
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
                        uploadRecord: inputs.OBJ_UR2,
                        partRecords: undefined
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

        testMpuVerifyStream(args);
});

test('parts but no UR: one batch', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_1,
                        records: [
                                inputs.OBJ_FR1,
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_PR1[1]
                        ]
                }
        ];

        var output = [];

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

        testMpuVerifyStream(args);
});

test('parts but no UR: multiple batches', function (t) {
        var input = [
                // valid batch
                {
                        uploadId: inputs.ID_0,
                        records: [
                                inputs.OBJ_FR0,
                                inputs.OBJ_UR0,
                                inputs.OBJ_PR0[0]
                        ]
                },
                // valid batch
                {
                        uploadId: inputs.ID_2,
                        records: [
                                inputs.OBJ_FR2,
                                inputs.OBJ_UR2
                        ]
                },
                // invalid batch
                {
                        uploadId: inputs.ID_1,
                        records: [
                                inputs.OBJ_FR1,
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_PR1[1],
                                inputs.OBJ_PR1[2]
                        ]
                }
        ];

        var output = [
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
                        uploadRecord: inputs.OBJ_UR2,
                        partRecords: undefined
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

        testMpuVerifyStream(args);
});
