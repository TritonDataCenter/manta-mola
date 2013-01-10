// Copyright 2012 Joyent, Inc.  All rights reserved.

var carrier = require('carrier');
var events = require('events');
var moray = require('moray');
var util = require('util');



///--- Globals

var MORAY_BUCKET = 'manta_delete_log';
var MORAY_CONNECT_TIMEOUT = 1000;
var MORAY_PORT = 2020;



///--- API

/**
 * Given a stream of lines like so:
 *   moray + [TAB] + [moray hostname] + [TAB] + [objectId] + [TAB] +
 *     [delete time]
 * This class will connect to Moray and delete the records from the
 * manta_delete_log table.
 */
function MorayCleaner(opts, listener) {
        var self = this;
        var log = opts.log;
        var mantaClient = opts.mantaClient;
        var object = opts.object;
        var shard = opts.shard;
        var inProgress = 0;
        var endCalled = false;
        self.carrier = null;

        if (listener) {
                self.addListener('error', listener);
                self.addListener('end', listener);
        }

        var client = moray.createClient({
                log: opts.log,
                connectTimeout: MORAY_CONNECT_TIMEOUT,
                host: shard,
                port: MORAY_PORT
        });
        self.client = client;

        function tryEnd() {
                if (inProgress < 1 && endCalled) {
                        client.close();
                }
        }

        client.on('connect', function () {
                var maxRequests = 20;
                opts.log.info({ shard: shard }, 'Connected to shard.');
                mantaClient.get(object, {}, function (err, stream) {
                        if (err) {
                                log.error(err);
                                return;
                        }

                        self.carrier = carrier.carry(stream);

                        self.carrier.on('line', function (line) {
                                ++inProgress;
                                if (inProgress > maxRequests) {
                                        stream.pause();
                                }
                                var ms = 'moray';
                                if (line.slice(0, ms.length) === ms) {
                                        log.info({
                                                line: line
                                        }, 'deleting from moray');

                                        function df(err2) {
                                                if (err2) {
                                                        self.emit('error', err);
                                                }
                                                --inProgress;
                                                if (inProgress < maxRequests) {
                                                        stream.resume();
                                                }
                                                tryEnd();
                                        }

                                        deleteFromMoray(opts, self, line, df);
                                }
                        });

                        self.carrier.on('error', function (err2) {
                                self.emit('error', err2);
                        });

                        self.carrier.on('end', function () {
                                endCalled = true;
                                tryEnd();
                        });
                });
        });

        client.on('error', function (err) {
                self.emit('error', err2);
        });

        client.on('close', function () {
                self.emit('end');
        });
}

util.inherits(MorayCleaner, events.EventEmitter);
module.exports = MorayCleaner;



///--- Helpers

function deleteFromMoray(opts, self, line, cb) {
        var log = opts.log;
        log.info({ line: line }, 'Processing line.');
        var parts = line.split('\t');
        if (parts.length < 4) {
                log.error({
                        line: line,
                        object: opts.object
                }, 'Is supposed to be a moray gc line,');
                //TODO: What?!
                cb({ name: 'BadLine', message: 'Bad Line' });
        }
        var shard = parts[1];
        var objectId = parts[2];
        var time = parts[3];
        var key = '/' + objectId + '/' + time;

        self.client.delObject(MORAY_BUCKET, key, {}, function (err2) {
                if (err2 && err2.name !== 'ObjectNotFoundError') {
                        log.error(err2, 'Err deleting in Moray');
                        cb(err2);
                }
                log.debug({ line: line }, 'GCed.');
                cb();
        });
}
