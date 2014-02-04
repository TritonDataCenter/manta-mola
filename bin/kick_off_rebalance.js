#!/usr/bin/env node
// -*- mode: js -*-
// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var common = require('../lib/common');
var fs = require('fs');
var getopt = require('posix-getopt');
var lib = require('../lib');
var manta = require('manta');
var moray = require('moray');
var path = require('path');



///--- Global Objects

var NAME = 'mola-rebalance';
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: NAME,
        stream: process.stdout
});
var MOLA_REBALANCE_CONFIG = (process.env.MOLA_REBALANCE_CONFIG ||
                   '/opt/smartdc/mola/etc/config.json');
var MOLA_REBALANCE_CONFIG_OBJ = JSON.parse(
        fs.readFileSync(MOLA_REBALANCE_CONFIG));
var MANTA_CLIENT = manta.createClientFromFileSync(MOLA_REBALANCE_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;
var MORAY_BUCKET = 'manta_storage';
var MORAY_CONNECT_TIMEOUT = 1000;
var MORAY_PORT = 2020;



///--- Global Constants
var MP = '/' + MANTA_USER + '/stor';
var MANTA_DUMP_NAME_PREFIX = 'manta-';
var SHARKS_ASSET_FILE = '/var/tmp/mola-rebalance-sharks.json';



///--- Helpers

/* BEGIN JSSTYLED */
function getEnvCommon(opts) {
        return (' \
set -o pipefail && \
cd /assets/ && gtar -xzf ' + opts.marlinPathToAsset + ' && cd mola && \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
/**
 * The map phase takes the pgdump, filters out all the directories
 * and formats them like:
 * [objectid] { [json pg row data] }
 */
function getMapCmd(opts) {
        return (getEnvCommon(opts) + ' \
gzcat -f | ./build/node/bin/node ./bin/pg_transform.js | \
   ./build/node/bin/node ./bin/jext.js -f objectid -x | \
   msplit -d " " -f 1 -n ' + opts.numberReducers + ' \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getRebalanceCmd(opts) {
        var sharksAsset = '/assets' + opts.sharksAssetObject;
        var tmpDir = '/var/tmp/sharkDist';
        var hostOption = '';
        if (opts.host) {
                hostOption = '-h ' + opts.host + ' ';
        }
        return (getEnvCommon(opts) + ' \
mkdir ' + tmpDir + ' && \
sort | ./build/node/bin/node ./bin/jext.js -r | \
    ./build/node/bin/node ./bin/rebalance.js \
       -s ' + sharksAsset + ' -d ' + tmpDir + ' ' + hostOption + '&& \
for i in $(ls ' + tmpDir + '); do \
   mmkdir ' + opts.jobRoot + '/do/$i; \
   mput -f ' + tmpDir + '/$i \
     ' + opts.jobRoot + '/do/$i/$MANTA_JOB_ID-X-$(uuid); \
done \
');
}
/* END JSSTYLED */


function parseOptions() {
        var option;
        //First take what's in the config file, override what's on the
        // command line, and use the defaults if all else fails.
        var opts = MOLA_REBALANCE_CONFIG_OBJ;
        opts.shards = opts.shards || [];
        opts.ignoreSharks = [];
        var parser = new getopt.BasicParser('a:h:i:m:nr:s:t',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'a':
                        opts.assetFile = option.optarg;
                        break;
                case 'h':
                        opts.host = option.optarg;
                        break;
                case 'i':
                        opts.ignoreSharks.push(option.optarg);
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 'n':
                        opts.noJobStart = true;
                        break;
                case 'r':
                        opts.marlinMemory = parseInt(option.optarg, 10);
                        break;
                case 's':
                        opts.storageShard = option.optarg;
                        break;
                case 't':
                        opts.jobName = 'manta_rebalance_test';
                        opts.jobRoot = MP + '/manta_rebalance_test';
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        if (!opts.storageShard) {
                usage('Storage shard is required.');
        }

        //Set up some defaults...
        opts.jobName = opts.jobName || 'manta_rebalance';
        opts.jobRoot = opts.jobRoot || MP + '/manta_rebalance';
        opts.directories = [
                opts.jobRoot + '/do'
        ];
        opts.assetDir = opts.jobRoot + '/assets';
        opts.assetObject = opts.assetDir + '/mola.tar.gz';
        opts.sharksAssetObject = opts.assetDir + '/sharks.json';
        opts.assetFile = opts.assetFile ||
                '/opt/smartdc/common/bundle/mola.tar.gz';

        opts.marlinMemory = opts.marlinMemory || 4096;
        opts.marlinPathToAsset = opts.assetObject.substring(1);
        opts.marlinAssetObject = opts.assetObject;

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-a asset_object]';
        str += ' [-h manta_storage_id]';
        str += ' [-m moray_shard]';
        str += ' [-n no_job_start]';
        str += ' [-r marlin_memory]';
        str += ' [-s storage_shard]';
        str += ' [-t output_to_test]';
        console.error(str);
        process.exit(1);
}


//Pulls the current set of active sharks and uploads to manta.  I probably
// should have the job manager do this, but whatever.
function makeSharksAsset(opts, cb) {
        getCurrentSharks(opts, function (err, sharks) {
                if (err) {
                        cb(err);
                        return;
                }
                if (Object.keys(sharks).length < 1) {
                        var message = 'no active sharks found.';
                        cb(new Error(message));
                        return;
                }
                //TODO: Could just use an in-memory object...
                fs.writeFileSync(SHARKS_ASSET_FILE, JSON.stringify(sharks));
                var stats = fs.statSync(SHARKS_ASSET_FILE);
                var o = {
                        copies: 2,
                        size: stats.size
                };

                var s = fs.createReadStream(SHARKS_ASSET_FILE);
                var p = opts.sharksAssetObject;
                s.pause();
                s.on('open', function () {
                        MANTA_CLIENT.put(p, s, o, function (e) {
                                fs.unlinkSync(SHARKS_ASSET_FILE);
                                cb(e);
                        });
                });
        });
}


function getRebalanceJob(opts, cb) {
        makeSharksAsset(_opts, function (err, sharks) {
                //As a first pass, the number of reducers should be the same
                // as the number of pg shards we have.
                opts.numberReducers = opts.objects.length;

                if (err) {
                        finish(err);
                        return;
                }

                var job = {
                        phases: [ {
                                type: 'storage-map',
                                memory: opts.marlinMemory,
                                exec: getMapCmd(opts)
                        }, {
                                type: 'reduce',
                                count: opts.numberReducers,
                                memory: opts.marlinMemory,
                                exec: getRebalanceCmd(opts),
                                assets: [ opts.sharksAssetObject ]
                        } ]
                };

                LOG.info({ job: job }, 'Rebalance Marlin Job Definition');

                cb(null, job);
        });
}


function findObjects(opts, cb) {
        common.findTablesForShards({
                mantaUser: MANTA_USER,
                mantaClient: MANTA_CLIENT,
                shards: opts.shards,
                tablePrefixes: [ MANTA_DUMP_NAME_PREFIX ]
        }, function (err, objs) {
                if (err) {
                        cb(err);
                        return;
                }
                cb(null, objs);
        });
}


function getCurrentSharks(opts, cb) {
        var client = moray.createClient({
                log: LOG,
                connectTimeout: MORAY_CONNECT_TIMEOUT,
                host: opts.storageShard,
                port: MORAY_PORT
        });

        client.on('connect', function () {
                var sharks = {};
                var req = client.findObjects(MORAY_BUCKET,
                                             '(manta_storage_id=*)', {});

                req.once('error', function (err) {
                        cb(err);
                        return;
                });

                req.on('record', function (obj) {
                        var dc = obj.value.datacenter;
                        var mantaStorageId = obj.value.manta_storage_id;
                        //Filter out host if we're migrating away from it.
                        if (mantaStorageId === opts.host) {
                                return;
                        }
                        //Filter out other ignore hosts
                        if (opts.ignoreSharks.indexOf(mantaStorageId) !== -1) {
                                return;
                        }
                        if (!sharks[dc]) {
                                sharks[dc] = [];
                        }
                        sharks[dc].push({
                                'manta_storage_id': mantaStorageId,
                                'datacenter': dc
                        });
                });

                req.once('end', function () {
                        client.close();
                        cb(null, sharks);
                });
        });
}



///--- Main

var _opts = parseOptions();

_opts.getJobDefinition = getRebalanceJob;
_opts.getJobObjects = findObjects;

function finish(err) {
        if (err) {
                LOG.fatal(err);
                process.exit(1);
        }
        MANTA_CLIENT.close();
        LOG.info('Done for now.');
        process.exit(0);
}

var jobManager = lib.createJobManager(_opts, MANTA_CLIENT, LOG);
jobManager.run(function () {
        finish();
});
