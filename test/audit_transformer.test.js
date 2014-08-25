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
        var mantaKey = '/poseidon/stor/mako/2.stor.coal.joyent.us';
        var fileName = 'data/audit_rt_test/mako_sample';
        var readStream = fs.createReadStream(fileName, {encoding: 'ascii'});
        var opts = {
                reader: readStream,
                mantaKey: mantaKey
        };
        var pt = lib.createAuditRowTransformer(opts);

        var count = 0;
        pt.on('row', function (row) {
                ++count;
                assert.ok(row.objectId);
                assert.ok(row.storageId);
                assert.string(row.type, 'mako');
                var s = row.objectId + '\t' + row.storageId + '\tmako';
                assert.ok(row.toString() === s, 'Expected <<' + s +
                          '>> but got <<' + row.toString() + '>>');
        });

        //wc -l ./data/audit_rt_test/mako_sample
        pt.once('end', function () {
                t.ok(count === 71, 'Count was ' + count);
                t.end();
        });

        t.end();
});


test('test: tranform moray', function (t) {
        var mantaKey = '/poseidon/stor/manatee_backups/' +
                '1.moray.coal.joyent.us/2013/05/09/18/' +
                'manta-2013-05-09-18-21-15.gz';
        var fileName = 'data/audit_rt_test/moray_sample';
        var readStream = fs.createReadStream(fileName, {encoding: 'ascii'});
        var opts = {
                reader: readStream,
                mantaKey: mantaKey
        };
        var pt = lib.createAuditRowTransformer(opts);

        var count = 0;
        pt.on('row', function (row) {
                ++count;
                assert.ok(row.objectId);
                assert.ok(row.storageId);
                assert.string(row.type, 'moray');
                var s = row.objectId + '\t' + row.storageId + '\tmoray\t' +
                        row.key + '\t' + row.shard;
                assert.ok(row.toString() === s, 'Expected <<' + s +
                          '>> but got <<' + row.toString() + '>>');
        });

        //cat data/audit_rt_test/moray_sample | grep 'entry' | \
        //   json -ga 'entry[3]' | json -ga 'sharks' | \
        //   grep manta_storage_id | wc -l
        pt.once('end', function () {
                t.ok(count === 52, 'Count was ' + count);
                t.end();
        });

        t.end();
});
