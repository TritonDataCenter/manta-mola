#!/usr/bin/env node
// -*- mode: js -*-
// Copyright 2012 Joyent, Inc.  All rights reserved.

var bunyan = require('bunyan');
var getopt = require('posix-getopt');
var lib = require('../lib');
var manta = require('manta');
var path = require('path');



///--- Globals

var MORAY_CLEANUP_PATH = '/manta_gc/moray';
var MANTA_CONFIG = (process.env.MANTA_CONFIG ||
                    '/opt/smartdc/common/etc/config.json');
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: 'moray_gc',
        stream: process.stdout
});
var MANTA_CLIENT = manta.createClientFromFileSync(MANTA_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;



///--- Helpers

function ifError(err) {
        if (err) {
                LOG.fatal(err);
                process.exit(1);
        }
}


function cleanShard(shard) {
        var dir = MORAY_CLEANUP_PATH + '/' + shard + '/do';
        LOG.info({ shard: shard, dir: dir }, 'Cleaning up shard.');
        MANTA_CLIENT.ls(dir, function (err, res) {
                ifError(err);

                res.on('object', function (obj) {
                        var o = dir + '/' + obj.name;
                        var opts = {
                                object: o,
                                mantaClient: MANTA_CLIENT,
                                log: LOG
                        };
                        var mc = lib.createMorayCleaner(opts);

                        mc.on('error', function (err2) {
                                LOG.error(err2, 'Error in Moray Cleaner');
                        });

                        mc.on('end', function () {
                                MANTA_CLIENT.unlink(o, {}, function (err2) {
                                        ifError(err2);
                                        LOG.info({ obj: o }, 'Done with obj,');
                                });
                        });
                });

                res.on('error', function (err2) {
                        ifError(err);
                });
        });
}



///--- Main

MANTA_CLIENT.ls(MORAY_CLEANUP_PATH, {}, function (err, res) {
        ifError(err);

        res.on('directory', function (dir) {
                var shard = dir.name;
                cleanShard(shard);
        });

        res.on('error', function (err2) {
                ifError(err2);
        });
});
