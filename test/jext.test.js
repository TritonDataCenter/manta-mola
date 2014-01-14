/*
 * jext.test.js: jext tests
 */

var fs = require('fs');
var child_process = require('child_process');

var bunyan = require('bunyan');
var helper = require('./helper.js');

var jext = './bin/jext.js';

var log = new bunyan({
    'name': 'jext.test.js',
    'level': process.env['LOG_LEVEL'] || 'debug'
});

var test = helper.test;

var objs = [
        { "name": { "first": "joe", "last": "johnson" }, "rank": "captain" },
        { "name": { "first": "bob", "last": "johnson" }, "rank": "ensign" },
        { "name": { "first": "sarah", "last": "miller" }, "rank": "commander" }
];
var ostrings = objs.map(function (o) {
        return (JSON.stringify(o, null, 0));
});
var SAMPLE_INPUT = ostrings.join('\n');

function runTest(opts, callback)
{
        var spawn = child_process.spawn(jext, opts.opts);
        var stdout = '';
        spawn.stdout.on('data', function (data) {
                stdout += data;
        });

        var stderr = '';
        spawn.stderr.on('data', function (data) {
                stderr += data;
        });

        var error = null;
        spawn.on('error', function (err) {
                error = err;
        });

        spawn.stdin.on('error', function (err) {
                error = err;
        });

        spawn.on('close', function (code) {
                var result = {
                        stdin: opts.stdin,
                        stdout: stdout,
                        stderr: stderr,
                        code: code,
                        error: error
                };
                if (opts.debug) {
                        console.log(result);
                }
                callback(result);
        });

        process.nextTick(function () {
                spawn.stdin.write(opts.stdin || '');
                spawn.stdin.end();
        });
}

test('testNoOpts', function (t)
{
        runTest({
                stdin: '',
                opts: []
        }, function (result) {
                t.equal(2, result.code);
                t.done();
        });
});


test('shallowField', function (t) {
        runTest({
                stdin: SAMPLE_INPUT,
                opts: ['-f', 'rank']
        }, function (result) {
                t.equal(0, result.code);
                var exp = [];
                for (var i = 0; i < objs.length; ++i) {
                        exp.push(objs[i]['rank'] + ' ' + ostrings[i]);
                }
                t.equal(exp.join('\n') + '\n', result.stdout);
                t.done();
        });

});


test('deepField', function (t) {
        runTest({
                stdin: SAMPLE_INPUT,
                opts: ['-f', 'name.first']
        }, function (result) {
                t.equal(0, result.code);
                var exp = [];
                for (var i = 0; i < objs.length; ++i) {
                        exp.push(objs[i]['name']['first'] + ' ' + ostrings[i]);
                }
                t.equal(exp.join('\n') + '\n', result.stdout);
                t.done();
        });

});


test('manyDeepField', function (t) {
        runTest({
                stdin: SAMPLE_INPUT,
                opts: ['-f', 'name.last', '-f', 'name.first']
        }, function (result) {
                t.equal(0, result.code);
                var exp = [];
                for (var i = 0; i < objs.length; ++i) {
                        exp.push(objs[i]['name']['last'] + ' ' +
                                 objs[i]['name']['first'] + ' ' +
                                 ostrings[i]);
                }
                t.equal(exp.join('\n') + '\n', result.stdout);
                t.done();
        });

});


test('testReverseOneField', function (t) {
        var s = [];
        for (var i = 0; i < objs.length; ++i) {
                s.push(objs[i]['name']['last'] + ' ' +
                       ostrings[i]);
        }

        runTest({
                stdin: s.join('\n'),
                opts: ['-r']
        }, function (result) {
                t.equal(0, result.code);
                t.equal(SAMPLE_INPUT + '\n', result.stdout);
                t.done();
        });

});


test('testReverseManyFields', function (t) {
        var s = [];
        for (var i = 0; i < objs.length; ++i) {
                s.push(objs[i]['name']['last'] + ' ' +
                       objs[i]['name']['first'] + ' ' +
                       ostrings[i]);
        }

        runTest({
                stdin: s.join('\n'),
                opts: ['-r']
        }, function (result) {
                t.equal(0, result.code);
                t.equal(SAMPLE_INPUT + '\n', result.stdout);
                t.done();
        });

});
