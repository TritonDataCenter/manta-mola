/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var fs = require('fs');
var helper = require('./helper.js');
var lib = require('../lib');



///--- Globals

var test = helper.test;



///--- Helpers

function checkSampleObject(t, n, obj) {
        t.equal('table', obj['__table'], 'Table names didn\'t match.');
        t.equal(n, obj.id, 'Id isn\'t correct.');
        t.equal('string' + n, obj.string, 'String isn\'t correct.');
        t.equal('objval' + n, obj.object['obj' + n], 'Object isn\'t correct');
        t.equal(null, obj.nullKey, 'Null wasn\'t null');
}



///--- Tests

test('test: sample schema', function (t) {
        var fileName = 'data/generic_table/dump.sample';
        var readStream = fs.createReadStream(fileName, {encoding: 'ascii'});
        var line = 0;

        lib.createSchemaReader(readStream, function (obj) {
                ++line;
                checkSampleObject(t, line, obj);
                if (line >= 2) {
                        t.ok(true);
                        t.end();
                }
        });
});


test('test: sample schema, on style', function (t) {
        var fileName = 'data/generic_table/dump.sample';
        var readStream = fs.createReadStream(fileName, {encoding: 'ascii'});
        var line = 0;

        var schemaReader = lib.createSchemaReader(readStream);

        function onObject(obj) {
                ++line;
                checkSampleObject(t, line, obj);
                if (line >= 2) {
                        t.ok(true);
                        t.end();
                }
        }

        schemaReader.on('object', onObject);
});


test('test: sample schema, garbage', function (t) {
        var fileName = 'data/generic_table/dump_garbage.sample';
        var readStream = fs.createReadStream(fileName, {encoding: 'ascii'});
        var line = 0;
        var error = false;

        var schemaReader = lib.createSchemaReader(readStream);

        function onObject(obj) {
                ++line;
                checkSampleObject(t, line, obj);
                if (line >= 2) {
                        t.ok(error);
                        t.end();
                }
        }

        schemaReader.on('object', onObject);
        schemaReader.on('error', function (err) {
                error = err.line && err.line.number === 3 &&
                        err.line.data === 'GARBAGEGARBAGE';
        });
});
