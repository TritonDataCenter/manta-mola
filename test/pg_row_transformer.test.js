//Copyright 2012 Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var fs = require('fs');
var helper = require('./helper.js');
var lib = require('../lib');



///--- Globals

var test = helper.test;



///--- Helpers

function checkDeadRow(row, moray_hostname) {
        //All the rest of this isn't strictly necessary, but might as
        // well leave it.
        assert.ok(row.objectId);
        assert.object(row.obj);
        var row_date = new Date(row.obj['_value'].mtime);
        assert.ok(row_date);
        assert.ok(row.date - 0 == row_date - 0, 'Date ' + row.date +
                  ' isnt the row date ' + row_date);
        assert.equal(row.type, 'dead');
        assert.equal(row.moray_hostname, moray_hostname, 'Moray hostname');
        var s = row.objectId + '\t' + row.date.toISOString() +
                '\tdead\t' + JSON.stringify(row.obj) + '\t' +
                moray_hostname;
        assert.equal(row.toString(), s, 'Expected <<' + s +
                     '>> but got <<' + row.toString() + '>>');
}



///--- Tests

test('test: tranform live', function (t) {
        var file_name = 'data/pg_rt_test/2012-10-05-16-00-02-manta';
        var read_stream = fs.createReadStream(file_name, {encoding: 'ascii'});
        var dump_date_string = '2012-10-05T16:00:02.000Z';
        var dump_date = new Date(dump_date_string);
        var moray_hostname = 'moray.localhost';
        var opts = {
                reader: read_stream,
                dump_date: dump_date,
                least_dump_date: dump_date,
                moray_hostname: moray_hostname
        };
        var pt = lib.createPgRowTransformer(opts);

        var count = 0;
        pt.on('row', function (row) {
                ++count;
                assert.ok(row.objectId);
                assert.ok(row.date === dump_date);
                assert.string(row.type, 'live');
                var s = row.objectId + '\t' + dump_date_string + '\tlive';
                assert.ok(row.toString() === s, 'Expected <<' + s +
                          '>> but got <<' + row.toString() + '>>');
        });

        //grep object data/pg_rt_test/2012-10-05-16-00-02-manta \
        //    | json -a entry[2] | json -a objectId | wc -l
        pt.once('end', function () {
                t.ok(count === 594, 'Count was ' + count);
                t.end();
        });
});

test('test: transform dead', function (t) {
        var file_name = 'data/pg_rt_test/2012-10-05-16-00-02-manta_delete_log';
        var read_stream = fs.createReadStream(file_name, {encoding: 'ascii'});
        var dump_date_string = '2012-10-05T16:00:02.000Z';
        var dump_date = new Date(dump_date_string);
        var moray_hostname = 'moray.localhost';
        var opts = {
                reader: read_stream,
                dump_date: dump_date,
                least_dump_date: dump_date,
                moray_hostname: moray_hostname
        };
        var pt = lib.createPgRowTransformer(opts);

        var count = 0;
        pt.on('row', function (row) {
                ++count;
                checkDeadRow(row, moray_hostname);
        });

        //cat data/pg_rt_test/2012-10-05-16-00-02-manta_delete_log \
        //    | json -a entry[2] | json -a objectId | wc -l
        pt.once('end', function () {
                t.ok(count === 128, 'Count was ' + count);
                t.end();
        });
});

test('test: transform dead, discard newest entries', function (t) {
        var file_name = 'data/pg_rt_test/2012-10-05-16-00-02-manta_delete_log';
        var read_stream = fs.createReadStream(file_name, {encoding: 'ascii'});
        var dump_date_string = '2012-10-05T16:00:02.000Z';
        var dump_date = new Date(dump_date_string);
        //cat data/pg_rt_test/2012-10-05-16-00-02-manta_delete_log \
        //   | json -a entry[2] | json -a mtime | sort -n \
        //   | head -50 | tail -1 | perl -ne 'print int($_ / 1000) + 1;' \
        //   | xargs -i date -d @{} +'%Y-%m-%dT%H:%M:%S.000Z'
        var least_dump_date_string = '2012-10-05T00:59:05.000Z';
        var least_dump_date = new Date(least_dump_date_string);
        var moray_hostname = 'moray.localhost';
        var opts = {
                reader: read_stream,
                dump_date: dump_date,
                least_dump_date: least_dump_date,
                moray_hostname: moray_hostname
        };
        var pt = lib.createPgRowTransformer(opts);

        var count = 0;
        pt.on('row', function (row) {
                ++count;
                checkDeadRow(row, moray_hostname);
        });

        pt.once('end', function () {
                t.ok(count === 50, 'Count was ' + count);
                t.end();
        });
});
