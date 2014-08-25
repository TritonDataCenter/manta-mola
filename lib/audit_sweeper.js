/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var carrier = require('carrier');
var events = require('events');
var http = require('http');
var moray = require('moray');
var util = require('util');
var vasync = require('vasync');



///--- Globals

var MORAY_BUCKET = 'manta';
var MORAY_CONNECT_TIMEOUT = 1000;
var MORAY_PORT = 2020;



///--- Object

/**
 * This class will sweep objects from an audit run.  It makes some assumptions:
 *
 * 1) The entire file can fit into memory.
 * 2) A shark format is as follows:
 *     {
 *         "manta_storage_id": "1.stor.us-east.joyent.us",
 *         "datacenter": "us-east-2"
 *     }
 */
function AuditSweeper(opts) {
        assert.object(opts.log, 'opts.log');
        assert.arrayOfObject(opts.storageList, 'opts.storageList');

        var self = this;
        self.log = opts.log;
        self.storageList = opts.storageList;
        self.morayClients = {};

        // Validate storageList
        for (var i = 0; i < self.storageList.length; ++i) {
                var s = self.storageList[i];
                var keys = Object.keys(s);
                if (keys.length !== 2 ||
                    keys.indexOf('manta_storage_id') === -1 ||
                    keys.indexOf('datacenter') === -1) {
                        throw new Error('Invalid Storage Descriptor: ' +
                                        JSON.stringify(s));
                }
        }

        // Stats
        self.stats = {
                loaded: 0,
                nonexistant: 0,
                alreadyModified: 0,
                modified: 0,
                etagConflicts: 0,
                errorLines: 0
        };
}
module.exports = AuditSweeper;



///--- Methods


AuditSweeper.prototype.getStats = function getStats() {
        return (this.stats);
};


AuditSweeper.prototype.getMorayClient = function getMorayClient(shard, cb) {
        var self = this;
        if (self.morayClients[shard]) {
                cb(self.morayClients[shard]);
                return;
        }

        var client = moray.createClient({
                log: self.log,
                connectTimeout: MORAY_CONNECT_TIMEOUT,
                host: shard,
                port: MORAY_PORT
        });

        client.on('connect', function () {
                self.log.info({ shard: shard }, 'Connected to shard.');
                if (!self.morayClients[shard]) {
                        self.morayClients[shard] = client;
                }
                cb(self.morayClients[shard]);
        });
};


/**
 * Closes all the moray clients this cleaner has a handle on.
 */
AuditSweeper.prototype.close = function close(cb) {
        var self = this;
        for (var shard in self.morayClients) {
                var c = self.morayClients[shard];
                c.close();
        }
        cb();
};


AuditSweeper.prototype.locateObject = function locateObject(morayObject, cb) {
        var self = this;

        var i = 0;
        var path = '/' + morayObject.value.owner + '/' +
                morayObject.value.objectId;
        var locs = [];
        function checkNext() {
                var storage = self.storageList[i];
                if (!storage) {
                        cb(null, locs);
                        return;
                }

                var opts = {
                        method: 'HEAD',
                        host: storage.manta_storage_id,
                        path: path,
                        port: 80
                };
                var req = http.request(opts, function (res) {
                        res.on('end', function () {
                                if (res.statusCode === 200 &&
                                    morayObject.value.contentLength ===
                                    parseInt(res.headers['content-length'], 10))
                                {
                                        locs.push(storage);
                                }
                                ++i;
                                checkNext();
                        });
                });

                req.once('error', function (err) {
                        cb(err);
                });

                req.end();
        }
        checkNext();
};


AuditSweeper.prototype.processLine = function processLine(line, cb) {
        var self = this;

        var okExitError = new Error('not really an error!');
        okExitError.notReally = true;
        var o = {};
        vasync.pipeline({
                funcs: [
                        function parseLine(_, subcb) {
                                var parts = line.split('\t');
                                o.obj = {
                                        id: parts[0],
                                        storageId: parts[1],
                                        key: parts[3],
                                        shard: parts[4]
                                };
                                subcb();
                        },
                        function fetchMorayClient(_, subcb) {
                                self.getMorayClient(o.obj.shard, function (c) {
                                        o.morayClient = c;
                                        subcb();
                                });
                        },
                        function fetchMorayRecord(_, subcb) {
                                var b = MORAY_BUCKET;
                                var k = o.obj.key;
                                var c = o.morayClient;
                                c.getObject(b, k, {}, function (e, mo) {
                                        if (e &&
                                            e.name === 'ObjectNotFoundError') {
                                                self.stats.nonexistant += 1;
                                                subcb(okExitError);
                                                return;
                                        }
                                        o.morayObject = mo;
                                        subcb(e);
                                });
                        },
                        function verifyCurrentSharks(_, subcb) {
                                var ss = o.morayObject.value.sharks;
                                for (var i = 0; i < ss.length; ++i) {
                                        var s = ss[i];
                                        var ks = Object.keys(s);
                                        if (ks.length !== 2 ||
                                            ks.indexOf('manta_storage_id') ===
                                            -1 ||
                                            ks.indexOf('datacenter') === -1) {
                                                var mo = o.morayObject;
                                                var m = 'shark data contains ' +
                                                        'more data than ' +
                                                        'anticipated';
                                                self.log.error({
                                                        morayObject: mo
                                                }, m);
                                                subcb(new Error(m));
                                                return;
                                        }
                                }
                                subcb();
                        },
                        function findObjectLocations(_, subcb) {
                                var mo = o.morayObject;
                                self.locateObject(mo, function (e, locs) {
                                        o.locations = locs;
                                        subcb(e);
                                });
                        },
                        function verifyDurabilityLevel(_, subcb) {
                                if (o.locations.length !==
                                    o.morayObject.value.sharks.length) {
                                        var m = 'durability levels don\'t ' +
                                                'match between moray data ' +
                                                'and locations.';
                                        self.log.error({
                                                morayObject: o.morayObject,
                                                locations: o.locations
                                        }, m);
                                        subcb(new Error(m));
                                        return;
                                }
                                subcb();
                        },
                        function verifySharksChanged(_, subcb) {
                                function mapper(s) {
                                        return (s['manta_storage_id']);
                                }

                                var ss = o.morayObject.value.sharks.map(mapper);
                                var ls = o.locations.map(mapper);
                                ss.sort();
                                ls.sort();
                                for (var i = 0; i < ss.length; ++i) {
                                        if (ss[i] !== ls[i]) {
                                                subcb();
                                                return;
                                        }
                                }
                                self.log.info({
                                        morayLocations: ss,
                                        makoLocations: ls
                                }, 'shark locations haven\'t changed.');
                                self.stats.alreadyModified += 1;
                                subcb(okExitError);
                        },
                        function updateSharks(_, subcb) {
                                self.log.info({
                                        obj: o.obj,
                                        mo: o.morayObject,
                                        locs: o.locations
                                }, 'updating object with new locations');
                                var etag = o.morayObject._etag;
                                if (!etag) {
                                        var m = 'moray object has no etag';
                                        self.log.error({
                                                morayObject: o.morayObject
                                        }, m);
                                        subcb(new Error(m));
                                        return;
                                }
                                var b = MORAY_BUCKET;
                                var k = o.morayObject.key;
                                var v = o.morayObject.value;
                                v.sharks = o.locations;
                                var c = o.morayClient;
                                var op = { etag: etag };
                                c.putObject(b, k, v, op, function (e) {
                                        var ece = 'EtagConflictError';
                                        if (e && e.name !== ece) {
                                                subcb(e);
                                                return;
                                        }
                                        if (e && e.name === ece) {
                                                self.log.info({
                                                        obj: o
                                                }, 'Etag conflict, run again.');
                                                self.stats.etagConflicts += 1;
                                        } else {
                                                self.stats.modified += 1;
                                        }
                                        subcb();
                                });
                        }
                ]
        }, function (err) {
                if (err && !err.notReally) {
                        cb(err);
                        return;
                }
                self.log.info({ line: line, obj: o.obj }, 'processed line');
                cb();
        });
};


AuditSweeper.prototype.processLines = function processLines(lines, cb) {
        var self = this;

        var errorLines = [];
        var i = 0;
        function processNext() {
                var line = lines[i];
                if (!line) {
                        cb(null, { errorLines: errorLines });
                        return;
                }

                self.processLine(line, function (err) {
                        if (err) {
                                self.log.error({ err: err,
                                                 line: line
                                               }, 'error with line');
                                self.stats.errorLines += 1;
                                errorLines.push(line);
                        }
                        ++i;
                        processNext();
                });
        }
        processNext();
};


/**
 * Given a stream of lines like so (see audit_row_transformer):
 *
 *     objectId + '\t' + storageId + '\tmoray\t' + key + '\t' + shard
 *
 * Will find the actual object location and clean up moray.
 */
AuditSweeper.prototype.run = function clean(opts, cb) {
        var self = this;
        var log = self.log;
        var reader = opts.reader;

        log.debug('foo');

        var runRes = {};
        var lines = [];
        vasync.pipeline({
                'funcs': [
                        function loadFromFile(_, subcb) {
                                var car = carrier.carry(reader);

                                car.on('line', function (line) {
                                        self.stats.loaded += 1;
                                        lines.push(line);
                                });

                                car.once('error', function (err) {
                                        subcb(err);
                                });

                                car.once('end', function () {
                                        subcb();
                                });

                                opts.reader.resume();
                        },
                        function processMorayRows(_, subcb) {
                                self.processLines(lines, function (err, res) {
                                        if (err) {
                                                subcb(err);
                                                return;
                                        }
                                        runRes = res;
                                        subcb();
                                });
                        }
                ]
        }, function (err) {
                if (err) {
                        cb(err);
                        return;
                }
                cb(null, runRes);
        });
};
