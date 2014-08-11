// Copyright 2012 Joyent, Inc.  All rights reserved.

var carrier = require('carrier');
var events = require('events');
var moray = require('moray');
var util = require('util');
var vasync = require('vasync');



///--- Globals

var MORAY_BUCKET = 'manta_delete_log';
var MORAY_CONNECT_TIMEOUT = 1000;
var MORAY_PORT = 2020;



///--- Object

/**
 * This class will connect to Moray and delete the records from the
 * manta_delete_log table.
 */
function MorayCleaner(opts, listener) {
        var self = this;
        this.log = opts.log;
        this.morayClients = {};
        this.rowsDeleted = 0;
        this.rowsAlreadyDeleted = 0;

        if (listener) {
                self.addListener('error', listener);
        }
}
util.inherits(MorayCleaner, events.EventEmitter);
module.exports = MorayCleaner;



///--- Helpers

function deleteFromMoray(opts, cb) {
        var self = opts.self;
        var lines = opts.lines;
        var expectedShard = opts.expectedShard;
        var client = opts.client;
        var ms = 'moray';

        var log = self.log;

        var keys = [];
        for (var i = 0; i < lines.length; ++i) {
                var line = lines[i];
                log.debug({ line: line }, 'Processing line.');
                var parts = line.split('\t');
                if (parts.length < 4) {
                        log.error({
                                line: line
                        }, 'Is supposed to be a moray gc line,');
                        cb({ name: 'BadLine', message: 'Bad Line' });
                }

                var type = parts[0];
                var shard = parts[1];
                var objectId = parts[2];
                var time = parts[3];
                var key = '/' + objectId + '/' + time;

                if (type !== ms) {
                        log.info({ line: line },
                                 'Line is not expected type.  Skipping.');
                        continue;
                }

                if (expectedShard !== shard) {
                        log.info({ shard: shard, expectedShard: expectedShard },
                                 'Skipping line because it isnt for the ' +
                                 'expected shard.');
                        continue;
                }

                keys.push(key);
        }

        if (keys.length < 1) {
                return (cb());
        }

        var filter = '(|';
        for (i = 0; i < keys.length; ++ i) {
                filter += '(_key=' + keys[i] + ')';
        }
        filter += ')';

        var startDate = new Date();
        client.deleteMany(MORAY_BUCKET, filter, function (err) {
                var endDate = new Date();
                var latency = endDate.getTime() - startDate.getTime();

                if (!err) {
                        self.rowsDeleted += keys.length;
                }

                log.info({
                        'audit': true,
                        'shard': expectedShard,
                        'lines': lines,
                        'keys': keys,
                        'rowsDeleted': err ? 0 : keys.length,
                        'error': err,
                        'latency': latency
                }, 'GC Audit.');

                cb();
        });
}



///--- Methods


MorayCleaner.prototype.getStats = function getStats() {
        return ({
                'rowsDeleted': this.rowsDeleted,
                'rowsAlreadyDeleted': this.rowsAlreadyDeleted
        });
};


MorayCleaner.prototype.getMorayClient = function getMorayClient(shard, cb) {
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

        //Yes, there's the possibility that there will be many of these.  Not
        // sure if I care yet.
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
MorayCleaner.prototype.close = function close(cb) {
        var self = this;
        for (var shard in self.morayClients) {
                var c = self.morayClients[shard];
                c.close();
        }
        cb();
};


/**
 * Given a stream of lines like so:
 *   moray + [TAB] + [moray hostname] + [TAB] + [objectId] + [TAB] +
 *     [delete time]
 *
 * Will clean the relevant lines from the manta_delete_log table of the Moray
 * shard.
 */
MorayCleaner.prototype.clean = function clean(opts, cb) {
        var self = this;
        var log = self.log;
        var shard = opts.shard;
        var stream = opts.stream;
        var object = opts.object;
        var maxInBatch = 100;

        log.debug({ object: object }, 'Moray Cleaner Object Entering.');

        var queue = vasync.queue(function (opts2, cb2) {
                var lines = opts2.lines;
                var client = opts2.client;

                deleteFromMoray({
                        self: self,
                        lines: lines,
                        expectedShard: shard,
                        client: client
                }, function (err) {
                        if (err) {
                                self.emit('error', err);
                        }
                        cb2();
                });
        }, 1);

        queue.on('end', cb);

        self.getMorayClient(shard, function (client) {
                var car = carrier.carry(stream);

                var lines = [];
                car.on('line', function (line) {
                        lines.push(line);
                        if (lines.length === maxInBatch) {
                                queue.push.call(queue, {
                                        lines: lines,
                                        client: client
                                });
                                lines = [];
                        }
                });

                car.on('error', function (err) {
                        self.emit('error', err);
                });

                car.on('end', function () {
                        log.debug({ object: object },
                                 'Carrier for object ended.');
                        queue.push.call(queue, {
                                lines: lines,
                                client: client
                        });
                        queue.close();
                });

                stream.resume();
        });
};
