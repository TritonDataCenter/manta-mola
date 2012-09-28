//Copyright 2012 Joyent, Inc. All rights reserved.

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
        var file_name = 'data/generic_table_dump.sample';
        var read_stream = fs.createReadStream(file_name, {encoding: 'ascii'});
        var line = 0;

        lib.createSchemaReader(read_stream, function (obj) {
                ++line;
                checkSampleObject(t, line, obj);
                if (line >= 2) {
                        t.ok(true);
                        t.end();
                }
        });
});


test('test: sample schema, on style', function (t) {
        var file_name = 'data/generic_table_dump.sample';
        var read_stream = fs.createReadStream(file_name, {encoding: 'ascii'});
        var line = 0;

        var schema_reader = lib.createSchemaReader(read_stream);

        function onObject(obj) {
                ++line;
                checkSampleObject(t, line, obj);
                if (line >= 2) {
                        t.ok(true);
                        t.end();
                }
        }

        schema_reader.on('object', onObject);
});


test('test: sample schema, garbage', function (t) {
        var file_name = 'data/generic_table_dump_garbage.sample';
        var read_stream = fs.createReadStream(file_name, {encoding: 'ascii'});
        var line = 0;
        var error = false;

        var schema_reader = lib.createSchemaReader(read_stream);

        function onObject(obj) {
                ++line;
                checkSampleObject(t, line, obj);
                if (line >= 2) {
                        t.ok(error);
                        t.end();
                }
        }

        schema_reader.on('object', onObject);
        schema_reader.on('error', function (err) {
                error = err.line_number === 3;
        });
});
