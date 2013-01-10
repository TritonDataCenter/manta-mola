#!/usr/bin/env node
// -*- mode: js -*-
// Copyright 2012 Joyent, Inc.  All rights reserved.

var bunyan = require('bunyan');
var getopt = require('posix-getopt');
var lib = require('../lib');
var manta = require('manta');
var path = require('path');



///--- Globals

var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: 'moray_gc',
        stream: process.stdout
});
var MANTA_CONFIG = (process.env.MANTA_CONFIG ||
                    '/opt/smartdc/common/etc/config.json');
var MANTA_CLIENT = manta.createClientFromFileSync(MANTA_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;
var MORAY_CLEANUP_PATH = '/' + MANTA_USER + '/stor/manta_gc/moray';



///--- Helpers

function ifError(err) {
        if (err) {
                LOG.fatal(err);
                process.exit(1);
        }
}


function morayCleanShard(shard, obj, cb) {
        var dir = MORAY_CLEANUP_PATH + '/' + shard;
        var o = dir + '/' + obj.name;
        var opts = {
                object: o,
                mantaClient: MANTA_CLIENT,
                shard: shard,
                log: LOG
        };
        LOG.info({ shard: shard, object: o }, 'Processing object.');
        var mc = lib.createMorayCleaner(opts);

        mc.on('error', function (err2) {
                ifError(err2);
        });

        mc.on('end', function () {
                MANTA_CLIENT.unlink(o, {}, function (err2) {
                        ifError(err2);
                        LOG.info({ obj: o }, 'Done with obj,');
                        cb();
                });
        });
}


function morayCleanObjects(shard, objects, cb) {
        if (objects.length < 1) {
                cb();
                return;
        }

        var obj = objects.shift();
        LOG.info({ objects: objects, obj: obj }, 'Going to clean shard.');
        morayCleanShard(shard, obj, function (err) {
                ifError(err);
                morayCleanObjects(shard, objects, cb);
        });
}


function cleanShard(shard, cb) {
        var dir = MORAY_CLEANUP_PATH + '/' + shard;
        LOG.info({ shard: shard, dir: dir }, 'Cleaning up shard.');
        MANTA_CLIENT.ls(dir, function (err, res) {
                ifError(err);

                var objects = [];

                res.on('object', function (obj) {
                        objects.push(obj);
                });

                res.on('error', function (err2) {
                        ifError(err);
                });

                res.on('end', function () {
                        morayCleanObjects(shard, objects, function (err3) {
                                cb(err3);
                        });
                });
        });
}

function cleanShards(shards) {
        if (shards.length < 1) {
                return;
        }

        var shard = shards.shift();
        cleanShard(shard, function (err) {
                ifError(err);
                cleanShards(shards);
        });
}



///--- Main

MANTA_CLIENT.ls(MORAY_CLEANUP_PATH, {}, function (err, res) {
        ifError(err);
        var shards = [];

        res.on('directory', function (dir) {
                var shard = dir.name;
                shards.push(shard);
        });

        res.on('error', function (err2) {
                if (err2 && err2.name === 'ResourceNotFoundError') {
                        LOG.info({ path: MORAY_CLEANUP_PATH },
                                 'No directories yet for manta gc.  Exiting.');
                        process.exit(0);
                }
                ifError(err2);
        });

        res.on('end', function () {
                LOG.info({ shards: shards }, 'Going to clean shards.');
                cleanShards(shards);
        });
});
