/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


///--- Globals

var assert = require('assert-plus');
var jsprim = require('jsprim');
var stream = require('stream');

var helper = require('../helper');
var inputs = require('./testInputs');
var mpu = require('../../lib/mpu');
var mulrs = require('../../lib/mpu/mpuUnlinkLiveRecordStream');
var test = helper.test;

var LOG = helper.createLogger('mpuUnlinkStream test');

///--- Helpers

function testMpuUnlinkLiveRecordStream(args) {
        assert.object(args, 'args');
        assert.arrayOfObject(args.input, 'args.input');
        assert.arrayOfObject(args.output, 'args.output');
        assert.func(args.testCb, 'args.testCb');
        assert.string(args.type, 'args.type');
        assert.ok(args.type === mulrs.MULRS_TYPE_PART ||
                args.type === mulrs.MULRS_TYPE_UPLOADDIR);
        assert.func(args.unlinkFunc, 'args.unlinkFunc');
        assert.func(args.getAccountByIdFunc, 'args.getAccountByIdFunc');
        assert.optionalBool(args.dryRun, 'args.dryRun');
        assert.optionalBool(args.verbose, 'args.verbose');

        var mockMahiClient = {
                getAccountById: args.getAccountByIdFunc
        };

        var mockMantaClient = {
                unlink: args.unlinkFunc
        };

        var mvs = new mpu.createMpuUnlinkLiveRecordStream({
                log: LOG,
                type: args.type,
                mantaClient: mockMantaClient,
                mahiClient: mockMahiClient,
                dryRun: args.dryRun,
                verbose: args.verbose
        });

        var vsOpts = {
                cb: args.testCb,
                expect: args.output
        };
        var vs = new inputs.ValidationStream(vsOpts);

        var r = new stream.Readable({
                objectMode: true
        });

        args.input.forEach(function (i) {
                r.push(i);
        });
        r.push(null);
        r.pipe(mvs).pipe(vs);
}

///--- Tests: upload directory

test('upload directory: one batch (no parts)', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_0,
                        finalizingRecord: inputs.OBJ_FR0,
                        uploadRecord: inputs.OBJ_UR0
                }
        ];

        var paths = [];
        var expected = [
                inputs.PATH_UR0
        ];
        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);
                var res = {
                        statusCode: 204
                };

                ucb(null, res);
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_0, 'uuid mismatch');
                gcb(null, {
                        account: {
                                login: inputs.ACCT_LOGIN_0
                        }
                });
        }

        var args = {
                input: input,
                output: input,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }

                        t.ok(jsprim.deepEqual(paths, expected));

                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_UPLOADDIR
        };

        testMpuUnlinkLiveRecordStream(args);
});

test('upload directory: one batch (3 parts)', function (t) {
        var input = [
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

        var paths = [];
        var expected = [
                inputs.PATH_UR1
        ];
        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);
                var res = {
                        statusCode: 204
                };

                ucb(null, res);
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_1, 'uuid mismatch');
                gcb(null, {
                        account: {
                                login: inputs.ACCT_LOGIN_1
                        }
                });
        }

        var args = {
                input: input,
                output: input,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }

                        t.ok(jsprim.deepEqual(paths, expected));
                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_UPLOADDIR
        };

        testMpuUnlinkLiveRecordStream(args);
});

test('upload directory: multiple batches', function (t) {
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
                        uploadRecord: inputs.OBJ_UR2
                },
                {
                        uploadId: inputs.ID_3,
                        finalizingRecord: inputs.OBJ_FR3,
                        uploadRecord: inputs.OBJ_UR3,
                        partRecords: [
                                inputs.OBJ_PR3[0]
                        ]
                }
        ];

        var paths = [];
        var expected = [
                inputs.PATH_UR0,
                inputs.PATH_UR1,
                inputs.PATH_UR2,
                inputs.PATH_UR3
        ];
        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);
                var res = {
                        statusCode: 204
                };

                ucb(null, res);
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_0 ||
                        uuid === inputs.ACCT_ID_1 ||
                        uuid == inputs.ACCT_ID_2, 'uuid mismatch');
                var login;
                if (uuid === inputs.ACCT_ID_0) {
                        login = inputs.ACCT_LOGIN_0;
                } else if (uuid === inputs.ACCT_ID_1) {
                        login = inputs.ACCT_LOGIN_1;
                } else {
                        login = inputs.ACCT_LOGIN_2;
                }

                gcb(null, {
                        account: {
                                login: login
                        }
                });
        }

        var args = {
                input: input,
                output: input,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        t.ok(jsprim.deepEqual(paths, expected));
                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_UPLOADDIR
        };

        testMpuUnlinkLiveRecordStream(args);
});

test('upload directory: multiple batches (one with no UR)', function (t) {
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
                        uploadId: inputs.ID_1,
                        finalizingRecord: inputs.OBJ_FR1
                },
                {
                        uploadId: inputs.ID_2,
                        finalizingRecord: inputs.OBJ_FR2,
                        uploadRecord: inputs.OBJ_UR2
                }
        ];

        var paths = [];
        var expected = [
                inputs.PATH_UR0,
                inputs.PATH_UR2
        ];
        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);
                var res = {
                        statusCode: 204
                };

                ucb(null, res);
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_0 ||
                        uuid == inputs.ACCT_ID_2, 'uuid mismatch');
                var login;
                if (uuid === inputs.ACCT_ID_0) {
                        login = inputs.ACCT_LOGIN_0;
                } else {
                        login = inputs.ACCT_LOGIN_2;
                }

                gcb(null, {
                        account: {
                                login: login
                        }
                });
        }

        var args = {
                input: input,
                output: input,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        t.ok(jsprim.deepEqual(paths, expected));
                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_UPLOADDIR
        };

        testMpuUnlinkLiveRecordStream(args);
});

test('upload directory: 404 returned during unlink', function (t) {
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
                        uploadRecord: inputs.OBJ_UR1
                }
        ];

        var output = [ input[0], input[2] ];

        var paths = [];
        var expected = [
                inputs.PATH_UR0,
                inputs.PATH_UR2,
                inputs.PATH_UR1
        ];
        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);

                if (p === inputs.PATH_UR2) {
                        ucb(new Error('simulated 404'), {
                                statusCode: 404
                        });
                } else {
                        var res = {
                                statusCode: 204
                        };

                        ucb(null, res);
                }
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_0 ||
                        uuid === inputs.ACCT_ID_1 ||
                        uuid == inputs.ACCT_ID_2, 'uuid mismatch');
                var login;
                if (uuid === inputs.ACCT_ID_0) {
                        login = inputs.ACCT_LOGIN_0;
                } else if (uuid === inputs.ACCT_ID_1) {
                        login = inputs.ACCT_LOGIN_1;
                } else {
                        login = inputs.ACCT_LOGIN_2;
                }

                gcb(null, {
                        account: {
                                login: login
                        }
                });
        }

        var args = {
                input: input,
                output: output,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        t.ok(jsprim.deepEqual(paths, expected));
                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_UPLOADDIR
        };

        testMpuUnlinkLiveRecordStream(args);
});


test('upload directory: error returned during unlink', function (t) {
        /*
         * Deliberately fail a request for one of the upload directory unlinks.
         * We expect to see that batch dropped from the stream, but everything
         * else to continue working.
         */
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
                        uploadRecord: inputs.OBJ_UR1
                }
        ];

        var output = [ input[0], input[2] ];

        var paths = [];
        var expected = [
                inputs.PATH_UR0,
                inputs.PATH_UR2,
                inputs.PATH_UR1
        ];
        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);

                // Fail one of the requests.
                if (p === inputs.PATH_UR2) {
                        ucb(new Error('simulated server error'), {
                                statusCode: 503
                        });
                } else {
                        var res = {
                                statusCode: 204
                        };

                        ucb(null, res);
                }
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_0 ||
                        uuid === inputs.ACCT_ID_1 ||
                        uuid == inputs.ACCT_ID_2, 'uuid mismatch');
                var login;
                if (uuid === inputs.ACCT_ID_0) {
                        login = inputs.ACCT_LOGIN_0;
                } else if (uuid === inputs.ACCT_ID_1) {
                        login = inputs.ACCT_LOGIN_1;
                } else {
                        login = inputs.ACCT_LOGIN_2;
                }

                gcb(null, {
                        account: {
                                login: login
                        }
                });
        }

        var args = {
                input: input,
                output: output,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        t.ok(jsprim.deepEqual(paths, expected));
                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_UPLOADDIR
        };

        testMpuUnlinkLiveRecordStream(args);
});

test('upload directory: error returned during getAccountById', function (t) {
        /*
         * Deliberately fail a request for one of the uuid lookups from mahi.
         * We expect to see that batch dropped from the stream, but everything
         * else to continue working.
         */
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
                        uploadRecord: inputs.OBJ_UR1
                }
        ];

        var output = [ input[0], input[2] ];

        var paths = [];
        var expected = [
                inputs.PATH_UR0,
                inputs.PATH_UR1
        ];
        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);

                var res = {
                        statusCode: 204
                };

                ucb(null, res);
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_0 ||
                        uuid === inputs.ACCT_ID_1 ||
                        uuid == inputs.ACCT_ID_2, 'uuid mismatch');

                var err, login;
                if (uuid === inputs.ACCT_ID_0) {
                        login = inputs.ACCT_LOGIN_0;
                } else if (uuid === inputs.ACCT_ID_1) {
                        login = inputs.ACCT_LOGIN_1;
                } else {
                        login = inputs.ACCT_LOGIN_2;
                        err = new Error('simulated mahi error');
                }

                gcb(err, {
                        account: {
                                login: login
                        }
                });
        }

        var args = {
                input: input,
                output: output,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        t.ok(jsprim.deepEqual(paths, expected));
                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_UPLOADDIR
        };

        testMpuUnlinkLiveRecordStream(args);
});



///--- Tests: part records

test('parts: one batch (1 part)', function (t) {
        var input = [
                {
                        uploadId: inputs.ID_1,
                        finalizingRecord: inputs.OBJ_FR1,
                        uploadRecord: inputs.OBJ_UR1,
                        partRecords: [
                                inputs.OBJ_PR1[0]
                        ]
                }
        ];

        var paths = [];
        var expected = [
                inputs.PATH_PR1[0]
        ];
        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);
                var res = {
                        statusCode: 204
                };

                ucb(null, res);
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_1, 'uuid mismatch');
                gcb(null, {
                        account: {
                                login: inputs.ACCT_LOGIN_1
                        }
                });
        }

        var args = {
                input: input,
                output: input,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }

                        t.ok(jsprim.deepEqual(paths, expected));
                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_PART
        };

        testMpuUnlinkLiveRecordStream(args);
});

test('parts: one batch (3 parts)', function (t) {
        var input = [
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

        var paths = [];
        var expected = [
                inputs.PATH_PR1[0],
                inputs.PATH_PR1[1],
                inputs.PATH_PR1[2]
        ];
        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);
                var res = {
                        statusCode: 204
                };

                ucb(null, res);
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_1, 'uuid mismatch');
                gcb(null, {
                        account: {
                                login: inputs.ACCT_LOGIN_1
                        }
                });
        }

        var args = {
                input: input,
                output: input,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }

                        t.ok(jsprim.deepEqual(paths, expected));
                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_PART
        };

        testMpuUnlinkLiveRecordStream(args);
});

test('parts: multiple batches (1 part, 3 parts, 0 parts, 1 part)',
function (t) {
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
                        uploadRecord: inputs.OBJ_UR2
                },
                {
                        uploadId: inputs.ID_3,
                        finalizingRecord: inputs.OBJ_FR3,
                        uploadRecord: inputs.OBJ_UR3,
                        partRecords: [
                                inputs.OBJ_PR3[0]
                        ]
                }
        ];

        var paths = [];
        var expected = [
                inputs.PATH_PR0[0],
                inputs.PATH_PR1[0],
                inputs.PATH_PR1[1],
                inputs.PATH_PR1[2],
                inputs.PATH_PR3[0]
        ];
        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);
                var res = {
                        statusCode: 204
                };

                ucb(null, res);
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_0 ||
                        uuid === inputs.ACCT_ID_1 ||
                        uuid == inputs.ACCT_ID_2, 'uuid mismatch');
                var login;
                if (uuid === inputs.ACCT_ID_0) {
                        login = inputs.ACCT_LOGIN_0;
                } else if (uuid === inputs.ACCT_ID_1) {
                        login = inputs.ACCT_LOGIN_1;
                } else {
                        login = inputs.ACCT_LOGIN_2;
                }

                gcb(null, {
                        account: {
                                login: login
                        }
                });
        }

        var args = {
                input: input,
                output: input,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        t.ok(jsprim.deepEqual(paths, expected));
                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_PART
        };

        testMpuUnlinkLiveRecordStream(args);
});

test('parts: 404 returned during unlink', function (t) {
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
                                inputs.OBJ_PR1[1]
                        ]
                }
        ];

        var output = [ input[0], input[1] ];

        var paths = [];
        var expected = [
                inputs.PATH_PR0[0],
                inputs.PATH_PR1[0],
                inputs.PATH_PR1[1]
        ];

        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);

                if (p === inputs.PATH_PR1[0]) {
                        ucb(new Error('simulated 404'), {
                                statusCode: 404
                        });
                } else {
                        var res = {
                                statusCode: 204
                        };

                        ucb(null, res);
                }
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_0 ||
                        uuid === inputs.ACCT_ID_1 ||
                        uuid == inputs.ACCT_ID_2, 'uuid mismatch');
                var login;
                if (uuid === inputs.ACCT_ID_0) {
                        login = inputs.ACCT_LOGIN_0;
                } else if (uuid === inputs.ACCT_ID_1) {
                        login = inputs.ACCT_LOGIN_1;
                } else {
                        login = inputs.ACCT_LOGIN_2;
                }

                gcb(null, {
                        account: {
                                login: login
                        }
                });
        }

        var args = {
                input: input,
                output: output,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        console.log(paths, expected);
                        t.ok(jsprim.deepEqual(paths, expected));
                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_PART
        };

        testMpuUnlinkLiveRecordStream(args);
});


test('parts: error returned during unlink', function (t) {
        /*
         * Deliberately fail a request for one of the part unlinks.
         * We expect to see that batch dropped from the stream, but everything
         * else to continue working.
         */
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
                                inputs.OBJ_PR1[1]
                        ]
                }
        ];

        var output = [ input[0], input[1] ];

        var paths = [];
        var expected = [
                inputs.PATH_PR0[0],
                inputs.PATH_PR1[0],
                inputs.PATH_PR1[1]
        ];
        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);

                // Fail one of the requests.
                if (p === inputs.PATH_PR1[0]) {
                        ucb(new Error('simulated server error'), {
                                statusCode: 503
                        });
                } else {
                        var res = {
                                statusCode: 204
                        };

                        ucb(null, res);
                }
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_0 ||
                        uuid === inputs.ACCT_ID_1 ||
                        uuid == inputs.ACCT_ID_2, 'uuid mismatch');
                var login;
                if (uuid === inputs.ACCT_ID_0) {
                        login = inputs.ACCT_LOGIN_0;
                } else if (uuid === inputs.ACCT_ID_1) {
                        login = inputs.ACCT_LOGIN_1;
                } else {
                        login = inputs.ACCT_LOGIN_2;
                }

                gcb(null, {
                        account: {
                                login: login
                        }
                });
        }

        var args = {
                input: input,
                output: output,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        t.ok(jsprim.deepEqual(paths, expected));
                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_PART
        };

        testMpuUnlinkLiveRecordStream(args);
});

test('parts: error returned during getAccountById', function (t) {
        /*
         * Deliberately fail a request for one of the uuid lookups from mahi.
         * We expect to see that batch dropped from the stream, but everything
         * else to continue working.
         */
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
                        uploadId: inputs.ID_1,
                        finalizingRecord: inputs.OBJ_FR1,
                        uploadRecord: inputs.OBJ_UR1,
                        partRecords: [
                                inputs.OBJ_PR1[0],
                                inputs.OBJ_PR1[1]
                        ]
                },
                {
                        uploadId: inputs.ID_2,
                        finalizingRecord: inputs.OBJ_FR2,
                        uploadRecord: inputs.OBJ_UR2
                }
        ];

        var output = [ input[0], input[2] ];

        var paths = [];
        var expected = [
                inputs.PATH_PR0[0]
        ];
        function unlink(p, opts, ucb) {
                t.ok(typeof (opts) === 'object');
                t.ok(jsprim.deepEqual(opts, {
                        query: {
                                allowMpuDeletes: true
                        }
                }));
                paths.push(p);

                var res = {
                        statusCode: 204
                };

                ucb(null, res);
        }

        function getAccountById(uuid, gcb) {
                t.ok(uuid === inputs.ACCT_ID_0 ||
                        uuid === inputs.ACCT_ID_1 ||
                        uuid == inputs.ACCT_ID_2, 'uuid mismatch');

                var err, login;
                if (uuid === inputs.ACCT_ID_0) {
                        login = inputs.ACCT_LOGIN_0;
                } else if (uuid === inputs.ACCT_ID_1) {
                        login = inputs.ACCT_LOGIN_1;
                        err = new Error('simulated mahi error');
                } else {
                        login = inputs.ACCT_LOGIN_2;
                }

                gcb(err, {
                        account: {
                                login: login
                        }
                });
        }

        var args = {
                input: input,
                output: output,
                testCb: function cb(ok, actual) {
                        t.ok(ok, 'valid stream output');
                        if (!ok) {
                                console.error('invalid output', actual);
                        }
                        t.ok(jsprim.deepEqual(paths, expected));
                        t.done();
                },
                unlinkFunc: unlink,
                getAccountByIdFunc: getAccountById,
                type: mulrs.MULRS_TYPE_PART
        };

        testMpuUnlinkLiveRecordStream(args);
});
