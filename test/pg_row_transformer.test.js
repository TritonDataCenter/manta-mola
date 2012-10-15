//Copyright 2012 Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var fs = require('fs');
var helper = require('./helper.js');
var lib = require('../lib');



///--- Globals

var test = helper.test;



///--- Helpers

function checkDeadRow(row, morayHostname) {
        //All the rest of this isn't strictly necessary, but might as
        // well leave it.
        assert.ok(row.objectId);
        assert.object(row.obj);
        var rowDate = new Date(row.obj['_value'].mtime);
        assert.ok(rowDate);
        assert.ok(row.date - 0 == rowDate - 0, 'Date ' + row.date +
                  ' isnt the row date ' + rowDate);
        assert.equal(row.type, 'dead');
        assert.equal(row.morayHostname, morayHostname, 'Moray hostname');
        var s = row.objectId + '\t' + row.date.toISOString() +
                '\tdead\t' + JSON.stringify(row.obj) + '\t' +
                morayHostname;
        assert.equal(row.toString(), s, 'Expected <<' + s +
                     '>> but got <<' + row.toString() + '>>');
}



///--- Tests

test('test: tranform live', function (t) {
        var fileName = 'data/pg_rt_test/2012-10-05-16-00-02-manta';
        var readStream = fs.createReadStream(fileName, {encoding: 'ascii'});
        var dumpDateString = '2012-10-05T16:00:02.000Z';
        var dumpDate = new Date(dumpDateString);
        var morayHostname = 'moray.localhost';
        var opts = {
                reader: readStream,
                dumpDate: dumpDate,
                leastDumpDate: dumpDate,
                morayHostname: morayHostname
        };
        var pt = lib.createPgRowTransformer(opts);

        var count = 0;
        pt.on('row', function (row) {
                ++count;
                assert.ok(row.objectId);
                assert.ok(row.date === dumpDate);
                assert.string(row.type, 'live');
                var s = row.objectId + '\t' + dumpDateString + '\tlive';
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
        var fileName = 'data/pg_rt_test/2012-10-05-16-00-02-manta_delete_log';
        var readStream = fs.createReadStream(fileName, {encoding: 'ascii'});
        var dumpDateString = '2012-10-05T16:00:02.000Z';
        var dumpDate = new Date(dumpDateString);
        var morayHostname = 'moray.localhost';
        var opts = {
                reader: readStream,
                dumpDate: dumpDate,
                leastDumpDate: dumpDate,
                morayHostname: morayHostname
        };
        var pt = lib.createPgRowTransformer(opts);

        var count = 0;
        pt.on('row', function (row) {
                ++count;
                checkDeadRow(row, morayHostname);
        });

        //cat data/pg_rt_test/2012-10-05-16-00-02-manta_delete_log \
        //    | json -a entry[2] | json -a objectId | wc -l
        pt.once('end', function () {
                t.ok(count === 128, 'Count was ' + count);
                t.end();
        });
});


test('test: transform dead, discard newest entries', function (t) {
        var fileName = 'data/pg_rt_test/2012-10-05-16-00-02-manta_delete_log';
        var readStream = fs.createReadStream(fileName, {encoding: 'ascii'});
        var dumpDateString = '2012-10-05T16:00:02.000Z';
        var dumpDate = new Date(dumpDateString);
        //cat data/pg_rt_test/2012-10-05-16-00-02-manta_delete_log \
        //   | json -a entry[2] | json -a mtime | sort -n \
        //   | head -50 | tail -1 | perl -ne 'print int($_ / 1000) + 1;' \
        //   | xargs -i date -d @{} +'%Y-%m-%dT%H:%M:%S.000Z'
        var leastDumpDateString = '2012-10-05T00:59:05.000Z';
        var leastDumpDate = new Date(leastDumpDateString);
        var morayHostname = 'moray.localhost';
        var opts = {
                reader: readStream,
                dumpDate: dumpDate,
                leastDumpDate: leastDumpDate,
                morayHostname: morayHostname
        };
        var pt = lib.createPgRowTransformer(opts);

        var count = 0;
        pt.on('row', function (row) {
                ++count;
                checkDeadRow(row, morayHostname);
        });

        pt.once('end', function () {
                t.ok(count === 50, 'Count was ' + count);
                t.end();
        });
});
