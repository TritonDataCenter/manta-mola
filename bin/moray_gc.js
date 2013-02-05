#!/usr/bin/env node
// -*- mode: js -*-
// Copyright 2012 Joyent, Inc.  All rights reserved.

var bunyan = require('bunyan');
var exec = require('child_process').exec;
var fs = require('fs');
var getopt = require('posix-getopt');
var lib = require('../lib');
var manta = require('manta');
var path = require('path');


///--- Globals

var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'debug'),
        name: 'moray_gc',
        stream: process.stdout
});
var MANTA_CONFIG = (process.env.MANTA_CONFIG ||
                    '/opt/smartdc/common/etc/config.json');
var MANTA_CLIENT = manta.createClientFromFileSync(MANTA_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;
var MORAY_CLEANUP_PATH = '/' + MANTA_USER + '/stor/manta_gc/moray';
var MORAY_CLEANER = lib.createMorayCleaner({ log: LOG });
var PID_FILE = '/var/tmp/moray_gc.pid';
MORAY_CLEANER.on('error', function (err) {
        if (err) {
                LOG.fatal(err);
                process.exit(1);
        }
});



///--- Helpers

function morayCleanShard(shard, obj, cb) {
        var dir = MORAY_CLEANUP_PATH + '/' + shard;
        var o = dir + '/' + obj.name;

        LOG.info({ shard: shard, object: o }, 'Processing object.');

        MANTA_CLIENT.get(o, {}, function (err, stream) {
                if (err) {
                        cb(err);
                        return;
                }

                //The Moray Cleaner will unpause.
                stream.pause();

                MORAY_CLEANER.clean({
                        shard: shard,
                        stream: stream,
                        object: o
                }, function () {
                        MANTA_CLIENT.unlink(o, {}, function (err2) {
                                if (err2) {
                                        cb(err2);
                                        return;
                                }
                                LOG.info({ obj: o }, 'Done with obj,');
                                cb();
                        });
                });
        });
}

function morayCleanObjects(shard, objects, cb) {
        if (objects.length < 1) {
                cb();
                return;
        }

        var obj = objects.shift();
        LOG.info({ shard: shard, obj: obj }, 'Going to clean shard.');
        morayCleanShard(shard, obj, function (err) {
                if (err) {
                        cb(err);
                        return;
                }
                morayCleanObjects(shard, objects, cb);
        });
}


function cleanShard(shard, cb) {
        var dir = MORAY_CLEANUP_PATH + '/' + shard;
        LOG.info({ shard: shard, dir: dir }, 'Cleaning up shard.');
        MANTA_CLIENT.ls(dir, function (err, res) {
                if (err) {
                        cb(err);
                        return;
                }

                var objects = [];

                res.on('object', function (obj) {
                        objects.push(obj);
                });

                res.on('error', function (err2) {
                        cb(err2);
                });

                res.on('end', function () {
                        if (objects.length === 0) {
                                LOG.info({ shard: shard },
                                         'Shard already clean, ' +
                                         'no objects found.');
                                cb();
                                return;
                        }
                        morayCleanObjects(shard, objects, function (err3) {
                                cb(err3);
                        });
                });
        });
}


function cleanShards(shards, cb) {
        if (shards.length < 1) {
                cb();
                return;
        }

        var shard = shards.shift();
        cleanShard(shard, function (err) {
                if (err) {
                        cb(err);
                        return;
                }
                cleanShards(shards, cb);
        });
}


function startGc(cb) {
        MANTA_CLIENT.ls(MORAY_CLEANUP_PATH, {}, function (err, res) {
                if (err) {
                        cb(err);
                        return;
                }

                var shards = [];

                res.on('directory', function (dir) {
                        var shard = dir.name;
                        shards.push(shard);
                });

                res.on('error', function (err2) {
                        if (err2 && err2.name === 'ResourceNotFoundError') {
                                LOG.info({ path: MORAY_CLEANUP_PATH },
                                         'No directories yet for manta gc.');
                                cb();
                                return;
                        }
                        cb(err2);
                });

                res.on('end', function () {
                        LOG.info({ shards: shards }, 'Going to clean shards.');
                        cleanShards(shards, function (err2) {
                                cb(err2);
                        });
                });
        });
}


function checkAlreadyRunning(cb) {
        function recordPid() {
                LOG.debug('Taking process ownership.');
                fs.writeFileSync(PID_FILE, process.pid, 'utf8');
                startGc(function (err) {
                        cleanupPidFile(function () {
                                cb(err);
                        });
                });
        }

        fs.stat(PID_FILE, function (err, stat) {
                if (err && err.code === 'ENOENT') {
                        recordPid();
                        return;
                }

                if (err && err) {
                        cb(err);
                        return;
                }

                if (!stat.isFile()) {
                        recordPid();
                        return;
                }

                var pid = fs.readFileSync(PID_FILE, 'utf8');
                LOG.debug({ file: PID_FILE, foundPid: pid },
                          'Found process in pid file.');

                if (pid === '' || pid.length < 1) {
                        recordPid();
                        return;
                }

                exec('ps ' + pid, function (err2, stdout, stderr) {
                        if (err2) {
                                cb(err);
                                return;
                        }

                        LOG.debug({ stdout: stdout }, 'Got output.');
                        var lines = stdout.split('\n');
                        if (lines.length > 1 && lines[1].length > 0 &&
                            lines[1].indexOf(pid) !== -1) {
                                LOG.info({
                                        foundPid: pid
                                }, 'Moray GC process already running.');
                                cb();
                                return;
                        }
                        recordPid();
                        return;
                });
        });
}


function cleanupPidFile(cb) {
        fs.unlinkSync(PID_FILE);
        cb();
}


///--- Main

checkAlreadyRunning(function (err) {
        if (err) {
                LOG.fatal(err);
                process.exit(1);
        }
        MANTA_CLIENT.close();
        MORAY_CLEANER.close(function () {
                LOG.info('Done.');
        });
});
