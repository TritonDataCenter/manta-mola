#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var getopt = require('posix-getopt');
var lib = require('../lib');
var manta = require('manta');
var path = require('path');
var vasync = require('vasync');



///--- Global Objects

var NAME = 'pg_transform';
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: NAME,
        stream: process.stdout
});
var MOLA_CONFIG = (process.env.MOLA_CONFIG ||
                   '/opt/smartdc/mola/etc/config.json');
var MOLA_CONFIG_OBJ = JSON.parse(fs.readFileSync(MOLA_CONFIG));
var MANTA_CLIENT = manta.createClientFromFileSync(MOLA_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;



///--- Global Constants

var MP = '/' + MANTA_USER + '/stor';
var MORAY_TABLE_PREFIX = 'moray-';



///--- Helpers

/* BEGIN JSSTYLED */
function getEnvCommon(opts) {
        var cmd = ' \
set -o pipefail && \
cd /assets/ && gtar -xzf ' + opts.marlinPathToAsset + ' && cd mola && \
';
        if (opts.outputDirectory) {
                cmd += 'mkdir -p ' + opts.outputDirectory + ' && ';
        }
        return (cmd);
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getMapCmd(opts) {
        var odir = '$(dirname $MANTA_INPUT_OBJECT)';
        if (opts.outputDirectory) {
                odir = opts.outputDirectory;
        }
        var cmd = getEnvCommon(opts) + ' \
gzcat -f | ./build/node/bin/node bin/sqltojson.js -a \
   -r ' + odir + ' \
   -l /var/tmp \
';
        return (cmd);
}
/* END JSSTYLED */


function parseOptions() {
        var option;
        //First take what's in the config file, override what's on the
        // command line, and use the defaults if all else fails.
        var opts = MOLA_CONFIG_OBJ;
        opts.shards = opts.shards || [];
        var parser = new getopt.BasicParser('a:b:m:no:p:y:', process.argv);

        while ((option = parser.getopt()) !== undefined) {
                if (option.error) {
                        usage();
                }

                switch (option.option) {
                case 'a':
                        opts.assetFile = option.optarg;
                        break;
                case 'b':
                        opts.backfill = option.optarg;
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 'n':
                        opts.noJobStart = true;
                        break;
                case 'o':
                        opts.outputDirectory = option.optarg;
                        break;
                case 'p':
                        opts.pgMapDisk = lib.common.parseNumberOption(
                            option.optarg, '-p', 1, null, usage);
                        break;
                case 'y':
                        opts.pgMapMemory = lib.common.parseNumberOption(
                            option.optarg, '-y', 1, null, usage);
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        //Set up some defaults...
        opts.jobNamePrefix = opts.jobNamePrefix || 'manta_pg_transform';

        //This is the root for all of the transform jobs.  We'll split them
        // out later...
        opts.jobRoot = opts.jobRoot || MP + '/manta_pg_transform';

        opts.assetDir = opts.jobRoot + '/assets';
        opts.assetObject = opts.assetDir + '/mola.tar.gz';
        opts.assetFile = opts.assetFile ||
                '/opt/smartdc/common/bundle/mola.tar.gz';

        opts.jobEnabled = opts.pgEnabled;
        opts.marlinPathToAsset = opts.assetObject.substring(1);
        opts.marlinAssetObject = opts.assetObject;

        //In places other than coal, the marlin and storage shard may not be
        // in the list of shards...
        if (opts.marlinShard &&
            opts.shards.indexOf(opts.marlinShard) === -1) {
                opts.shards.push(opts.marlinShard);
        }
        if (opts.storageShard &&
            opts.shards.indexOf(opts.storageShard) === -1) {
                opts.shards.push(opts.storageShard);
        }

        opts.pgMapDisk = opts.pgMapDisk || 16;
        opts.pgMapMemory = opts.pgMapMemory || 1024;

        if (opts.outputDirectory &&
            !lib.common.endsWith(opts.outputDirectory, '/')) {
                    opts.outputDirectory += '/';
            }

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-a asset_file]';
        str += ' [-b backfill_object]';
        str += ' [-m moray_shard (can be repeated)]';
        str += ' [-n no_job_start]';
        str += ' [-o output_directory_prefix (defaults to object location)]';
        str += '';
        str += 'The backfill object overrides the shard list.';
        console.error(str);
        process.exit(1);
}


function getJob(opts, cb) {
        // The input determines where the output goes and the command to run...

        var job = {
                phases: [ {
                        type: 'map',
                        memory: opts.pgMapMemory,
                        disk: opts.pgMapDisk,
                        exec: getMapCmd(opts)
                } ]
        };

        cb(null, job);
}


function startJobForObject(opts, cb) {
        assert.object(opts, 'opts');
        assert.string(opts.shard, 'opts.shard');
        assert.object(opts.object, 'opts.object');
        assert.object(opts.opts, 'opts.opts');

        var shard = opts.shard;
        var object = opts.object;
        opts = opts.opts;

        var shardNum = shard.split('\.')[0];
        var dsub = object.parent.split('/').slice(5).join('_');
        var jobName = opts.jobNamePrefix + '-' + shardNum + '-' + dsub;
        var jjobRoot = opts.jobRoot + '/' + shard;
        /**
         * For example:
         *    jobName: manta_pg_transform-1-2014/11/03/17
         *    jobRoot: /poseidon/stor/manta_pg_transform/\
         *             1.moray.coal.joyent.us
         *    ...
         */
        var jopts = {
                'jobName': jobName,
                'jobRoot': jjobRoot,
                'morayDumpObject': object,
                'getJobObjects': function (_, subcb) {
                        return (subcb(null, [ object.path ]));
                },
                'getJobDefinition': getJob,
                'directories': [
                        opts.jobRoot
                ],
                // Passthrough options...
                'assetFile': opts.assetFile,
                'assetObject': opts.assetObject,
                'pgMapDisk': opts.pgMapDisk,
                'pgMapMemory': opts.pgMapMemory,
                'marlinPathToAsset': opts.marlinPathToAsset,
                'marlinAssetObject': opts.marlinAssetObject,
                'noJobStart': opts.noJobStart
        };
        if (opts.outputDirectory) {
                jopts.outputDirectory = opts.outputDirectory + shard;
        }
        LOG.info(jopts, 'Creating job manager');
        var jobManager = lib.createJobManager(jopts, MANTA_CLIENT, LOG);
        jobManager.run(cb);
}


function startShardTransform(opts, cb) {
        var shard = opts.shard;
        opts = opts.opts;

        // We do this a bit backward here... first we find the dumps, then
        // we instantiate the job manager.  This is because we want to detect
        // old jobs are still running.
        lib.common.findShardObjects({
                'shard': shard,
                'client': MANTA_CLIENT,
                'tablePrefixes': [ MORAY_TABLE_PREFIX ]
        }, function (err, objects) {
                if (err) {
                        return (cb(err));
                }

                if (objects.length < 1) {
                        return (cb(new Error('Couldnt find dump for shard ' +
                                             shard)));
                }

                //Take the last one in the hour, in case there were more than
                // one dump in the hour.
                var object = objects[objects.length - 1];
                startJobForObject({
                        'shard': shard,
                        'object': object,
                        'opts': opts
                }, cb);
        });
}


function startBackfill(mantaPath, opts, cb) {
        MANTA_CLIENT.info(mantaPath, function (err, object) {
                if (err) {
                        return (cb(err));
                }
                // /poseidon/stor/manatee_backups/[shard]/...
                var shard = mantaPath.split('/')[4];
                //Need to add a couple things...
                var dir = path.dirname(mantaPath);
                object.directory = dir;
                object.parent = dir;
                object.path = mantaPath;
                startJobForObject({
                        'shard': shard,
                        'object': object,
                        'opts': opts
                }, cb);
        });
}



///--- Main

var _opts = parseOptions();

function onDone(err) {
        if (err) {
                LOG.error(err);
        }
        MANTA_CLIENT.close();
        LOG.info('Done for now.');
}

if (_opts.backfill) {
        startBackfill(_opts.backfill, _opts, onDone);
} else {
        //For each shard, we need to create a job manager that will find the
        // right dumps
        vasync.forEachPipeline({
                'inputs': _opts.shards.map(function (s) {
                        return ({ 'shard': s, 'opts': _opts });
                }),
                'func': startShardTransform
        }, onDone);
}
