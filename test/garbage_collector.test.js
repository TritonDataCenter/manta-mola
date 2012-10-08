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
var PAST_GRACE_PERIOD_MILLIS = 60 * 60 * 24 * 2 * 1000 + 1000;
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
        var owner = 'owner-uuid';
        var obj = {
                'dirname': '/' + owner + '/stor',
                'key': '/' + owner + '/stor/key-1',
                'mtime': date.getTime(),
                'owner': owner,
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
        var actionsCalled = false;

        gc.on('actions', function (actions) {
                actionsCalled = true;
        });

        gc.on('end', function () {
                t.ok(!actionsCalled);
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

        gc.on('actions', function (actions) {
                assert.equal(actions.length, 1, 'Actions.length != 1');
                var action = actions[0];
                assert.equal(action.type, 'moray');
                assert.equal(action.moray_hostname, MORAY_1);
                assert.equal(action.objectId, '1234');
                var dateCheck = (new Date(now + 1000));
                assert.equal(action.date - 0, dateCheck - 0);
        });

        gc.on('end', function () {
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

        gc.on('actions', function (actions) {
                assert.equal(actions.length, 1, 'Actions.length != 1');
                var action = actions[0];
                assert.equal(action.type, 'moray');
                assert.equal(action.moray_hostname, MORAY_1);
                assert.equal(action.objectId, '1234');
                var dateCheck = (new Date(now + 1000));
                assert.equal(action.date - 0, dateCheck - 0);
        });

        gc.on('end', function () {
                t.end();
        });
});

test('test: dead object, before grace period', function (t) {
        var now = Date.now();
        var data = dead('1234', new Date(now), MORAY_1);
        var stream = new DummyStream(data);
        var gc = lib.createGarbageCollector(stream);
        var actionsCalled = false;

        gc.on('actions', function (actions) {
                actionsCalled = true;
        });

        gc.on('end', function () {
                t.ok(!actionsCalled);
                t.end();
        });
});

test('test: dead object, close before grace period', function (t) {
        var now = Date.now();
        var data = dead('1234', new Date(now - GRACE_PERIOD_MILLIS + 1000),
                        MORAY_1);
        var stream = new DummyStream(data);
        var gc = lib.createGarbageCollector(stream);
        var actionsCalled = false;

        gc.on('actions', function (actions) {
                actionsCalled = true;
        });

        gc.on('end', function () {
                t.ok(!actionsCalled);
                t.end();
        });
});

test('test: dead object, close after grace period', function (t) {
        var now = Date.now();
        var data = dead('1234', new Date(now - GRACE_PERIOD_MILLIS - 1000),
                     MORAY_1);
        var stream = new DummyStream(data);
        var gc = lib.createGarbageCollector(stream);
        var actionsCalled = false;

        gc.on('actions', function (actions) {
                actionsCalled = true;
        });

        gc.on('end', function () {
                t.ok(actionsCalled);
                t.end();
        });
});

/*
test('test: dead object', function (t) {
        var now = Date.now();
        var data = dead('1234', new Date(now - PAST_GRACE_PERIOD_MILLIS),
                     MORAY_1);
        var stream = new DummyStream(data);
        var gc = lib.createGarbageCollector(stream);

        gc.on('actions', function (actions) {
                console.log(JSON.stringify(actions));
        });

        gc.on('end', function () {
                t.end();
        });
});
*/

//all objects to gc- all dead and past grace period
//no object to gc- all dead but before grace period
//last object should be gced
//complicated
