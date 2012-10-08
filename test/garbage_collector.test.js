//Copyright 2012 Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var events = require('events');
var helper = require('./helper.js');
var lib = require('../lib');
var util = require('util');



///--- Globals

var GRACE_PERIOD_MILLIS = 60 * 60 * 24 * 2 * 1000; //2 days
var MORAY_1 = '1.moray.coal.joyent.us';
var MORAY_2 = '2.moray.coal.joyent.us';
var OWNER = 'owner-uuid';
var test = helper.test;



///--- Helpers

function DummyStream(data, listener) {
        var self = this;

        if (listener) {
                self.addListener('data', listener);
                self.addListener('end', listener);
        }

        self.setEncoding = function () {};

        process.nextTick(function () {
                self.emit('data', data);
                self.emit('end');
        });
}

util.inherits(DummyStream, events.EventEmitter);




///--- Helpers

function live(objectId, date) {
        return (objectId + '\t' + date.toISOString() + '\tlive\n');
}

function dead(objectId, date, moray_hostname) {
        var obj = {
                'dirname': '/' + OWNER + '/stor',
                'key': '/' + OWNER + '/stor/key-1',
                'mtime': date.getTime(),
                'owner': OWNER,
                'type': 'object',
                'contentLength': 3060,
                'contentMD5': 'l/niJjQMwQsp/TdHOYIgXg==',
                'contentType': 'application/octet-stream',
                'etag': objectId,
                'objectId': objectId,
                'sharks': [ {
                        'url': 'http://1.stor.coal.joyent.us',
                        'server_uuid': 'server',
                        'zone_uuid': '1'
                }, {
                        'url': 'http://2.stor.coal.joyent.us',
                        'server_uuid': 'server',
                        'zone_uuid': '2'
                }]
        };
        return (objectId + '\t' + date.toISOString() + '\tdead\t' +
                JSON.stringify(obj) + '\t' + moray_hostname + '\n');
}

function checkMoray(moray, moray_hostname, objectId, date) {
        assert.equal(moray.moray_hostname, moray_hostname);
        assert.equal(moray.objectId, objectId);
        assert.equal(moray.date - 0, date - 0);
}



///--- Tests

test('test: dummy stream', function (t) {
        var stream = new DummyStream('echo');
        var dataRead = false;

        stream.on('data', function (d) {
                assert.equal(d, 'echo');
                dataRead = true;
        });

        stream.on('end', function () {
                t.ok(dataRead);
                t.end();
        });
});

test('test: all live', function (t) {
        var now = Date.now();
        var data = live('1234', new Date(now)) +
                live('1234', new Date(now + 1000)) +
                live('4321', new Date(now));
        var stream = new DummyStream(data);
        var gc = lib.createGarbageCollector(stream);
        var morayCalled = false;
        var makoCalled = false;

        gc.on('moray', function (moray) {
                morayCalled = true;
        });

        gc.on('mako', function (mako) {
                makoCalled = true;
        });

        gc.on('end', function () {
                t.ok(!morayCalled);
                t.ok(!makoCalled);
                t.end();
        });
});

test('test: single moray cleanup, live object after', function (t) {
        var now = Date.now();
        var data = live('1234', new Date(now)) +
                dead('1234', new Date(now + 1000), MORAY_1) +
                live('1234', new Date(now + 2000));
        var stream = new DummyStream(data);
        var gc = lib.createGarbageCollector(stream);
        var morayCalled = 0;
        var makoCalled = false;

        gc.on('moray', function (moray) {
                ++morayCalled;
                assert.equal(moray.moray_hostname, MORAY_1);
                assert.equal(moray.objectId, '1234');
                var dateCheck = (new Date(now + 1000));
                assert.equal(moray.date - 0, dateCheck - 0);
        });

        gc.on('mako', function (mako) {
                makoCalled = true;
        });

        gc.on('end', function () {
                assert.equal(morayCalled, 1);
                assert.ok(!makoCalled);
                t.end();
        });
});

test('test: single moray cleanup, dead object after', function (t) {
        var now = Date.now();
        var data = live('1234', new Date(now)) +
                dead('1234', new Date(now + 1000), MORAY_1) +
                dead('1234', new Date(now + 2000), MORAY_2);
        var stream = new DummyStream(data);
        var gc = lib.createGarbageCollector(stream);
        var morayCalled = 0;
        var makoCalled = false;

        gc.on('moray', function (moray) {
                ++morayCalled;
                checkMoray(moray, MORAY_1, '1234', new Date(now + 1000));
        });

        gc.on('mako', function (mako) {
                makoCalled = true;
        });

        gc.on('end', function () {
                assert.equal(morayCalled, 1);
                assert.ok(!makoCalled);
                t.end();
        });
});

test('test: dead object, before grace period', function (t) {
        var now = Date.now();
        var data = dead('1234', new Date(now), MORAY_1);
        var stream = new DummyStream(data);
        var gc = lib.createGarbageCollector(stream);
        var morayCalled = false;
        var makoCalled = false;

        gc.on('moray', function (moray) {
                morayCalled = true;
        });

        gc.on('mako', function (mako) {
                makoCalled = true;
        });

        gc.on('end', function () {
                t.ok(!makoCalled);
                t.ok(!morayCalled);
                t.end();
        });
});

test('test: dead object, close before grace period', function (t) {
        var now = Date.now();
        var data = dead('1234', new Date(now - GRACE_PERIOD_MILLIS + 1000),
                        MORAY_1);
        var stream = new DummyStream(data);
        var gc = lib.createGarbageCollector(stream);
        var morayCalled = false;
        var makoCalled = false;

        gc.on('moray', function (moray) {
                morayCalled = true;
        });

        gc.on('mako', function (mako) {
                makoCalled = true;
        });

        gc.on('end', function () {
                t.ok(!makoCalled);
                t.ok(!morayCalled);
                t.end();
        });
});

test('test: dead object, close after grace period', function (t) {
        var now = Date.now();
        var recordDate = new Date(now - GRACE_PERIOD_MILLIS - 1000);
        var data = dead('1234', recordDate, MORAY_1);
        var stream = new DummyStream(data);
        var gc = lib.createGarbageCollector(stream);
        var morayCalled = 0;
        var makoCalled = 0;
        var makos = [];

        gc.on('moray', function (moray) {
                ++morayCalled;
                checkMoray(moray, MORAY_1, '1234', recordDate);
        });

        gc.on('mako', function (mako) {
                ++makoCalled;
                makos.push(mako);
        });

        gc.on('end', function () {
                for (var i = 1; i <= 2; ++i) {
                        var mako = makos[i - 1];
                        var mako_url = 'http://' + i + '.stor.coal.joyent.us';
                        assert.equal(mako_url, mako.url);
                        assert.equal('server', mako.server_uuid);
                        assert.equal(i, mako.zone_uuid);
                        assert.equal(OWNER, mako.owner);
                        assert.equal('1234', mako.objectId);
                }
                assert.equal(makoCalled, 2);
                assert.equal(morayCalled, 1);
                t.end();
        });
});
