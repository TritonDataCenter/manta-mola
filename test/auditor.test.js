//Copyright 2012 Joyent, Inc. All rights reserved.

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
