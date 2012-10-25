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
        self.carrier = null;

        if (listener) {
                self.addListener('error', listener);
                self.addListener('end', listener);
        }

        mantaClient.get(object, {}, function (err, stream) {
                if (err) {
                        log.error(err);
                        return;
                }

                self.carrier = carrier.carry(stream);

                self.carrier.on('line', function (line) {
                        var ms = 'moray';
                        if (line.slice(0, ms.length) === ms) {
                                log.info({ line: line }, 'deleting from moray');
                                deleteFromMoray(opts, line, function (err2) {
                                        if (err2) {
                                                self.emit('error', err);
                                        }
                                });
                        }
                });

                self.carrier.on('error', function (err2) {
                        self.emit('error', err2);
                });

                self.carrier.on('end', function () {
                        log.info({ object: object }, 'GC end.');
                        self.emit('end');
                });
        });
}

util.inherits(MorayCleaner, events.EventEmitter);
module.exports = MorayCleaner;



///--- Helpers

function deleteFromMoray(opts, line, cb) {
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

        //TODO: Reuse client?
        var client = moray.createClient({
                log: opts.log,
                connectTimeout: MORAY_CONNECT_TIMEOUT,
                host: shard,
                port: MORAY_PORT
        });

        client.on('connect', function () {
                client.delObject(MORAY_BUCKET, key, {}, function (err) {
                        if (err && err.name !== 'ObjectNotFoundError') {
                                log.error(err, 'Err deleting in Moray');
                                cb(err);
                        }
                        log.debug({ line: line }, 'GCed.');
                        client.close();
                        cb();
                });
        });

        client.on('error', function (err) {
                cb(err);
        });

        client.on('close', function () {
                cb();
        });
}
