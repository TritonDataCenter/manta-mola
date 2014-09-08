/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var fs = require('fs');
var helper = require('./helper.js');
var lib = require('../lib');



///--- Globals

var test = helper.test;



///--- Tests

test('test: tranform mako', function (t) {
        var mantaKey = '/poseidon/stor/mako/1.stor.staging.joyent.us';
        var fileName = 'data/cruft_rt_test/mako_sample';
        var readStream = fs.createReadStream(fileName, {encoding: 'ascii'});
        var opts = {
                reader: readStream,
                mantaKey: mantaKey,
                filterTimestamp: 1400000000
        };
        var ct = lib.createCruftRowTransformer(opts);

        var count = 0;
        ct.on('row', function (row) {
                ++count;
                assert.ok(row.objectId);
                assert.ok(row.storageId);
                assert.string(row.type, 'mako');
                var prefix = row.objectId + '\t' + row.type + '\t';
                assert.ok(row.toString().indexOf(prefix) === 0);
        });

        //grep -vE $'\t140' ./data/cruft_rt_test/mako_sample | wc -l
        ct.once('end', function () {
                t.ok(count === 34, 'Count was ' + count);
                t.end();
        });
});


test('test: tranform moray manta', function (t) {
        var mantaKey = '/poseidon/stor/manatee_backups/' +
                '2.moray.staging.joyent.us/2014/09/04/18/' +
                'manta-2014-09-04-20-35-19.gz';
        var fileName = 'data/cruft_rt_test/moray-manta_sample';
        var readStream = fs.createReadStream(fileName, {encoding: 'ascii'});
        var opts = {
                reader: readStream,
                mantaKey: mantaKey,
                filterTimestamp: 1400000000
        };
        var ct = lib.createCruftRowTransformer(opts);

        var count = 0;
        ct.on('row', function (row) {
                ++count;
                assert.ok(row.objectId);
                assert.string(row.type, 'moray');
                var s = row.objectId + '\tmoray';
                assert.ok(row.toString() === s, 'Expected <<' + s +
                          '>> but got <<' + row.toString() + '>>');
        });

        //grep entry data/cruft_rt_test/moray-manta_sample | \
        //   grep 'object' | wc -l
        ct.once('end', function () {
                t.ok(count === 24, 'Count was ' + count);
                t.end();
        });
});


test('test: tranform moray manta delete log', function (t) {
        var mantaKey = '/poseidon/stor/manatee_backups/' +
                '2.moray.staging.joyent.us/2014/09/04/18/' +
                'manta_delete_log-2014-09-04-20-35-45.gz';
        var fileName = 'data/cruft_rt_test/moray-manta_delete_log_sample';
        var readStream = fs.createReadStream(fileName, {encoding: 'ascii'});
        var opts = {
                reader: readStream,
                mantaKey: mantaKey,
                filterTimestamp: 1400000000
        };
        var ct = lib.createCruftRowTransformer(opts);

        var count = 0;
        ct.on('row', function (row) {
                ++count;
                assert.ok(row.objectId);
                assert.string(row.type, 'moray');
                var s = row.objectId + '\tmoray';
                assert.ok(row.toString() === s, 'Expected <<' + s +
                          '>> but got <<' + row.toString() + '>>');
        });

        //grep entry data/cruft_rt_test/moray-manta_delete_log_sample | \
        //   grep 'object' | wc -l
        ct.once('end', function () {
                t.ok(count === 50, 'Count was ' + count);
                t.end();
        });
});
