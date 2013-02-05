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

        if (listener) {
                self.addListener('error', listener);
        }
}
util.inherits(MorayCleaner, events.EventEmitter);
module.exports = MorayCleaner;



///--- Helpers

function deleteFromMoray(opts, cb) {
        var self = opts.self;
        var line = opts.line;
        var expectedShard = opts.expectedShard;
        var client = opts.client;

        var log = self.log;
        log.debug({ line: line }, 'Processing line.');
        var parts = line.split('\t');
        if (parts.length < 4) {
                log.error({
                        line: line
                }, 'Is supposed to be a moray gc line,');
                //TODO: What?!
                cb({ name: 'BadLine', message: 'Bad Line' });
        }
        var shard = parts[1];
        var objectId = parts[2];
        var time = parts[3];
        var key = '/' + objectId + '/' + time;

        if (expectedShard !== shard) {
                log.info({ shard: shard, expectedShard: expectedShard },
                         'Skipping line because it isnt for the expected ' +
                         'shard.');
                cb();
                return;
        }

        client.delObject(MORAY_BUCKET, key, {}, function (err2) {
                var alreadyDeleted = (err2 &&
                                      err2.name === 'ObjectNotFoundError');
                if (err2 && !alreadyDeleted) {
                        log.error(err2, 'Err deleting in Moray');
                        cb(err2);
                }
                log.info({ line: line, alreadyDeleted: alreadyDeleted },
                         'GCed.');
                cb();
        });
}



///--- Methods

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
        var maxRequests = 20;
        var endCalled = false;

        log.debug({ object: object }, 'Moray Cleaner Object Entering.');

        var queue = vasync.queue(function (opts2, cb2) {
                var ms = 'moray';
                var line = opts2.line;
                var client = opts2.client;
                if (line.slice(0, ms.length) !== ms) {
                        return;
                }

                log.debug({
                        line: line
                }, 'deleting from moray');

                deleteFromMoray({
                        self: self,
                        line: line,
                        expectedShard: shard,
                        client: client
                }, function (err2) {
                        if (err2) {
                                self.emit('error', err2);
                        }
                        cb2();
                });
        }, maxRequests);

        function tryEnd() {
                log.debug({ queue: queue.npending,
                            endCalled: endCalled,
                            object: object
                          }, 'Trying to end.');
                if (queue.npending === 0 && endCalled) {
                        cb();
                }
        }

        self.getMorayClient(shard, function (client) {
                var car = carrier.carry(stream);

                car.on('line', function (line) {
                        queue.push({
                                line: line,
                                client: client
                        }, tryEnd);
                });

                car.on('error', function (err2) {
                        self.emit('error', err2);
                });

                car.on('end', function () {
                        log.debug({ object: object },
                                 'Carrier for object ended.');
                        endCalled = true;
                        tryEnd();
                });

                stream.resume();
        });
};
