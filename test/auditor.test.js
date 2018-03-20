/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var helper = require('./helper.js');
var lib = require('../lib');
var MemoryStream = require('memorystream');
var util = require('util');



///--- Globals

var test = helper.test;



///--- Helpers

function l(objectId, storageId, type, more) {
        var line = objectId + '\t' +
                storageId + '\t' +
                type;
        if (more) {
                line += '\t' + more;
        }
        return (line + '\n');

}



///--- Tests

test('test: no problem, durability 1', function (t) {
        var data = l('o1', 's1', 'mako') +
                l('o1', 's1', 'moray', 'foo');
        var stream = new MemoryStream(data);
        var auditor = lib.createAuditor({ reader: stream });
        var problem = false;

        auditor.on('problem', function (p) {
                problem = true;
        });

        auditor.on('end', function () {
                t.ok(!problem);
                t.end();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('test: no problem, durability 2', function (t) {
        var data = l('o1', 's1', 'mako') +
                l('o1', 's2', 'mako') +
                l('o1', 's1', 'moray', 'foo') +
                l('o1', 's2', 'moray', 'foo');
        var stream = new MemoryStream(data);
        var auditor = lib.createAuditor({ reader: stream });
        var problem = false;

        auditor.on('problem', function (p) {
                problem = true;
        });

        auditor.on('end', function () {
                t.ok(!problem);
                t.end();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('test: no problem, links', function (t) {
        var data = l('o1', 's1', 'mako') +
                l('o1', 's2', 'mako') +
                l('o1', 's1', 'moray', 'foo') +
                l('o1', 's2', 'moray', 'foo') +
                l('o1', 's1', 'moray', 'foo1') +
                l('o1', 's2', 'moray', 'foo1') +
                l('o1', 's1', 'moray', 'foo2') +
                l('o1', 's2', 'moray', 'foo2');
        var stream = new MemoryStream(data);
        var auditor = lib.createAuditor({ reader: stream });
        var problem = false;

        auditor.on('problem', function (p) {
                problem = true;
        });

        auditor.on('end', function () {
                t.ok(!problem);
                t.end();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('test: no problem, multipart upload parts', function (t) {
        var p1 = [
                '',
                '61567b8a-980d-4260-ff3e-91ee1a6b01c2',
                'uploads',
                'b38',
                'b385957d-ba86-4ea5-ee90-fbc6918fa842',
                '0'
        ].join('/');
        var p2 = [
                '',
                '61567b8a-980d-4260-ff3e-91ee1a6b01c2',
                'uploads',
                'b38',
                'b385957d-ba86-4ea5-ee90-fbc6918fa842',
                '1'
        ].join('/');
        var p3 = [
                '',
                '34f89fa3-c63f-4d25-b00c-9d3ad2f148b3',
                'uploads',
                'b',
                'b843370f-43a3-4579-808b-3e5d2d2e0632',
                '0'
        ].join('/');

        var data = l('o1', 's1', 'moray', p1) +
                l('o2', 's1', 'moray', p2) +
                l('o3', 's2', 'moray', p3);
        var stream = new MemoryStream(data);
        var auditor = lib.createAuditor({ reader: stream });
        var problem = false;

        auditor.on('problem', function (p) {
                problem = true;
        });

        auditor.on('end', function () {
                t.ok(!problem);
                t.end();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('test: no problem, non-MPU part items under top-level uploads dir',
function (t) {
        var data = l('o1', 's1', 'moray',
                        '/61567b8a-980d-4260-ff3e-91ee1a6b01c2/uploads/foo') +
                l('o2', 's1', 'moray',
                        '/61567b8a-980d-4260-ff3e-91ee1a6b01c2/uploads/b3/foo');
        var stream = new MemoryStream(data);
        var auditor = lib.createAuditor({ reader: stream });
        var problem = false;

        auditor.on('problem', function (p) {
                problem = true;
        });

        auditor.on('end', function () {
                t.ok(!problem);
                t.end();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('test: problem one missing', function (t) {
        var data = l('o1', 's1', 'mako') +
                l('o1', 's1', 'moray', 'foo') +
                l('o1', 's2', 'moray', 'foo');
        var stream = new MemoryStream(data);
        var auditor = lib.createAuditor({ reader: stream });
        var problem = false;
        var problems = [];

        auditor.on('problem', function (p) {
                problem = true;
                problems.push(p);
        });

        auditor.on('end', function () {
                t.ok(problem);
                assert.equal(1, problems.length);
                assert.equal('o1\ts2\tmoray\tfoo', problems[0]);
                t.end();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('test: missing at beginning', function (t) {
        var data = l('o1', 's1', 'moray', 'foo');
        var stream = new MemoryStream(data);
        var auditor = lib.createAuditor({ reader: stream });
        var problem = false;
        var problems = [];

        auditor.on('problem', function (p) {
                problem = true;
                problems.push(p);
        });

        auditor.on('end', function () {
                t.ok(problem);
                assert.equal(1, problems.length);
                assert.equal('o1\ts1\tmoray\tfoo', problems[0]);
                t.end();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('test: missing at end', function (t) {
        var data = l('o1', 's1', 'mako') +
                l('o1', 's1', 'moray', 'foo') +
                l('o2', 's1', 'moray', 'foo');
        var stream = new MemoryStream(data);
        var auditor = lib.createAuditor({ reader: stream });
        var problem = false;
        var problems = [];

        auditor.on('problem', function (p) {
                problem = true;
                problems.push(p);
        });

        auditor.on('end', function () {
                t.ok(problem);
                assert.equal(1, problems.length);
                assert.equal('o2\ts1\tmoray\tfoo', problems[0]);
                t.end();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('test: problem, paths similar to multipart upload parts', function (t) {
        // account not a uuid (one character missing)
        var a1 = [
                '',
                '61567b8a-980d-4260-ff3e-91ee1a6b012',
                'uploads',
                'b38',
                'b385957d-ba86-4ea5-ee90-fbc6918fa842',
                '0'
        ];
        var p1 = a1.join('/');

        // account not a uuid (account name)
        var a2 = [
                '',
                'poseidon',
                'uploads',
                'b38',
                'b385957-ba86-4ea5-ee90-fbc6918fa842',
                '1'
        ];
        var p2 = a2.join('/');

        // different top-level directory
        var a3 = [
                '',
                '34f89fa3-c63f-4d25-b00c-9d3ad2f148b3',
                'stor',
                'uploads',
                'b',
                'b843370f-43a3-4579-808b-3e5d2d2e0632',
                '0'
        ];
        var p3 = a3.join('/');

        var data =
                l('o1', 's1', 'moray', p1) +
                l('o2', 's1', 'moray', p2) +
                l('o3', 's2', 'moray', p3);

        var stream = new MemoryStream(data);
        var auditor = lib.createAuditor({ reader: stream });
        var problem = false;
        var problems = [];

        auditor.on('problem', function (p) {
                problem = true;
                problems.push(p);
        });

        auditor.on('end', function () {
                t.ok(problem);
                assert.equal(3, problems.length);
                t.end();
        });

        process.nextTick(function () {
                stream.end();
        });
});

test('test: multiple problems in stack', function (t) {
        var data = l('o1', 's1', 'mako') +
                l('o1', 's1', 'moray', 'foo') +
                l('o2', 's1', 'mako') +
                l('o2', 's1', 'moray', 'foo') +
                l('o3', 's1', 'mako') +
                l('o3', 's1', 'moray', 'foo') +
                l('o3', 's2', 'moray', 'foo') +
                l('o4', 's2', 'mako') +
                l('o4', 's1', 'moray', 'foo') +
                l('o5', 's3', 'mako') +
                l('o5', 's3', 'moray', 'foo');

        var stream = new MemoryStream(data);
        var auditor = lib.createAuditor({ reader: stream });
        var problem = false;
        var problems = [];

        auditor.on('problem', function (p) {
                problem = true;
                problems.push(p);
        });

        auditor.on('end', function () {
                t.ok(problem);
                assert.equal(2, problems.length);
                assert.equal('o3\ts2\tmoray\tfoo', problems[0]);
                assert.equal('o4\ts1\tmoray\tfoo', problems[1]);
                t.end();
        });

        process.nextTick(function () {
                stream.end();
        });
});
