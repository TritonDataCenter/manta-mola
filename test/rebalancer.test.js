//Copyright 2013 Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var helper = require('./helper.js');
var fs = require('fs');
var lib = require('../lib');
var MemoryStream = require('memorystream');
var util = require('util');



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;
var SHARKS = {
        '1': [
                { 'manta_storage_id': 'one', 'datacenter': '1' }
        ],
        '2': [
                { 'manta_storage_id': 'two', 'datacenter': '2' }
        ]
};
var TMP_DIR = '/var/tmp/mola-rebalancer-test';


///--- Helpers

function o(key, sharks) {
        return (JSON.stringify({
                type: 'object',
                _key: key,
                _etag: 'metag',
                _value: {
                        contentMD5: 'md5',
                        objectId: 'objectId',
                        owner: 'owner',
                        sharks: sharks,
                        etag: 'oetag'
                }
        }, null, 0) + '\n');
}



///--- Tests



before(function (cb) {
        var stat;
        var err;
        try {
                stat = fs.statSync(TMP_DIR);
        } catch (e) {
                err = e;
        }
        if (err && err.code === 'ENOENT') {
                fs.mkdirSync(TMP_DIR);
        } else if (err) {
                throw (err);
        } else if (!stat.isDirectory()) {
                cb(new Error('something at path ' + TMP_DIR +
                             ' already exists'));
        }
        cb();
});


after(function (cb) {
        var files = fs.readdirSync(TMP_DIR);
        for (var i = 0; i < files.length; ++i) {
                var f = files[i];
                fs.unlinkSync(TMP_DIR + '/' + f);
        }
        cb();
});


function runTest(opts, cb) {
        var stream = new MemoryStream(opts.data);
        var rebalancer = lib.createRebalancer({
                mantaStorageId: opts.mantaStorageId,
                reader: stream,
                sharks: SHARKS,
                dir: TMP_DIR
        });

        rebalancer.on('end', function () {
                var files = fs.readdirSync(TMP_DIR);
                var fileMap = {};
                for (var i = 0; i < files.length; ++i) {
                        var f = files[i];
                        var contents = fs.readFileSync(TMP_DIR + '/' + f,
                                                       'utf8');
                        var cs = contents.split('\n');
                        if (cs[cs.length - 1] === '') {
                                cs.pop();
                        }
                        fileMap[f] = cs.map(function (l) {
                                return (JSON.parse(l));
                        });

                }
                cb(null, fileMap);
        });

        process.nextTick(function () {
                stream.end();
        });
}

test('test: no rebalance', function (t) {
        var data = o('k1', [
                { 'manta_storage_id': 'one', 'datacenter': '1' },
                { 'manta_storage_id': 'two', 'datacenter': '2' }
        ]);

        runTest({ data: data }, function (err, res) {
                t.ok(Object.keys(res).length === 0);
                t.end();
        });
});


test('test: rebalance first', function (t) {
        var data =
                o('k1', [
                        { 'manta_storage_id': 'one', 'datacenter': '1' },
                        { 'manta_storage_id': 'one', 'datacenter': '1' }
                ]);

        runTest({ data: data }, function (err, res) {
                t.ok(Object.keys(res).length === 1);
                var filename = Object.keys(res)[0];
                t.equal('two', filename);
                var ob = res[filename];
                t.ok(ob.length === 1);
                var obj = ob[0];
                assert.deepEqual({
                        key: 'k1',
                        morayEtag: 'metag',
                        newShark: { manta_storage_id: 'two', datacenter: '2' },
                        oldShark: { manta_storage_id: 'one', datacenter: '1' },
                        md5: 'md5',
                        objectId: 'objectId',
                        owner: 'owner',
                        etag: 'oetag' }, obj);
                t.end();
        });
});


test('test: rebalance middle', function (t) {
        var data =
                o('k1', [
                        { 'manta_storage_id': 'one', 'datacenter': '1' },
                        { 'manta_storage_id': 'two', 'datacenter': '2' }
                ]) +
                o('k2', [
                        { 'manta_storage_id': 'one', 'datacenter': '1' },
                        { 'manta_storage_id': 'one', 'datacenter': '1' }
                ]) +
                o('k3', [
                        { 'manta_storage_id': 'two', 'datacenter': '2' },
                        { 'manta_storage_id': 'one', 'datacenter': '1' }
                ]);

        runTest({ data: data }, function (err, res) {
                t.ok(Object.keys(res).length === 1);
                var filename = Object.keys(res)[0];
                t.equal('two', filename);
                t.equal('k2', res[filename][0].key);
                t.end();
        });
});


test('test: rebalance last', function (t) {
        var data =
                o('k1', [
                        { 'manta_storage_id': 'one', 'datacenter': '1' },
                        { 'manta_storage_id': 'two', 'datacenter': '2' }
                ]) +
                o('k2', [
                        { 'manta_storage_id': 'two', 'datacenter': '2' },
                        { 'manta_storage_id': 'one', 'datacenter': '1' }
                ]) +
                o('k3', [
                        { 'manta_storage_id': 'one', 'datacenter': '1' },
                        { 'manta_storage_id': 'one', 'datacenter': '1' }
                ]);

        runTest({ data: data }, function (err, res) {
                t.ok(Object.keys(res).length === 1);
                var filename = Object.keys(res)[0];
                t.equal('two', filename);
                t.equal('k3', res[filename][0].key);
                t.end();
        });
});


test('test: Rebalance away, one shark', function (t) {
        var data =
                o('k1', [
                        { 'manta_storage_id': 'three', 'datacenter': '3' }
                ]);

        runTest({ data: data, mantaStorageId: 'three' }, function (err, res) {
                t.ok(Object.keys(res).length === 1);
                var filename = Object.keys(res)[0];
                t.ok(['one', 'two'].indexOf(filename) !== -1);
                var ob = res[filename];
                t.ok(ob.length === 1);
                var obj = ob[0];
                assert.deepEqual({
                        key: 'k1',
                        morayEtag: 'metag',
                        newShark: filename === 'one' ? SHARKS['1'][0] :
                                SHARKS['2'][0],
                        oldShark: { manta_storage_id: 'three',
                                    datacenter: '3' },
                        md5: 'md5',
                        objectId: 'objectId',
                        owner: 'owner',
                        etag: 'oetag' }, obj);
                t.end();
        });
});


test('test: Rebalance away, many sharks', function (t) {
        var data =
                o('k1', [
                        { 'manta_storage_id': 'two', 'datacenter': '2' },
                        { 'manta_storage_id': 'three', 'datacenter': '3' }
                ]);

        runTest({ data: data, mantaStorageId: 'three' }, function (err, res) {
                t.ok(Object.keys(res).length === 1);
                var filename = Object.keys(res)[0];
                t.equal('one', filename);
                var ob = res[filename];
                t.ok(ob.length === 1);
                var obj = ob[0];
                assert.deepEqual({
                        key: 'k1',
                        morayEtag: 'metag',
                        newShark: SHARKS['1'][0],
                        oldShark: { manta_storage_id: 'three',
                                    datacenter: '3' },
                        md5: 'md5',
                        objectId: 'objectId',
                        owner: 'owner',
                        etag: 'oetag' }, obj);
                t.end();
        });
});
