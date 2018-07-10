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



///--- Global Objects

var NAME = 'mola-cruft';
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: NAME,
        stream: process.stdout
});
var MOLA_CRUFT_CONFIG = (process.env.MOLA_CRUFT_CONFIG ||
                   '/opt/smartdc/mola/etc/config.json');
var MOLA_CRUFT_CONFIG_OBJ = JSON.parse(fs.readFileSync(MOLA_CRUFT_CONFIG));
var MANTA_CLIENT = manta.createClientFromFileSync(MOLA_CRUFT_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;



///--- Global Constants

var MP = '/' + MANTA_USER + '/stor';
var MANTA_DUMP_NAME_PREFIX = 'manta-';
var MANTA_DELETE_LOG_NAME_PREFIX = 'manta_delete_log-';
var DEFAULT_TOO_NEW_SECONDS = 60 * 60 * 24 * 2; // 2 days



///--- Helpers

function getEnvCommon(opts) {
        assert.string(opts.jobName, 'opts.jobName');
        assert.string(opts.marlinPathToAsset, 'opts.marlinPathToAsset');

/* BEGIN JSSTYLED */
        return (' \
set -o pipefail && \
export MANTA_CRUFT=' + opts.jobName + ' && \
export MARLIN_JOB=$(echo $MANTA_OUTPUT_BASE | cut -d "/" -f 4) && \
cd /assets/ && gtar -xzf ' + opts.marlinPathToAsset + ' && cd mola && \
');
/* END JSSTYLED */
}


function getTransformCmd(opts) {
        assert.number(opts.tooNewSeconds, 'opts.tooNewSeconds');
        assert.number(opts.cruftReducerCount, 'opts.cruftReducerCount');

        var grepForStorageNode = '';
        var filterTimestamp =
                Math.floor(opts.earliestMorayDump.getTime() / 1000) -
                opts.tooNewSeconds;
        if (opts.mantaStorageId) {
                grepForStorageNode = ' | grep ' + opts.mantaStorageId + ' | ';
        }

/* BEGIN JSSTYLED */
        return (getEnvCommon(opts) + ' \
gzcat -f | \
  ./build/node/bin/node ./bin/cruft_transform.js -k $MANTA_INPUT_OBJECT \
    -f ' + filterTimestamp + ' \
    ' + grepForStorageNode + ' | \
  msplit -n ' + opts.cruftReducerCount + ' \
');
/* END JSSTYLED */
}


/* BEGIN JSSTYLED */
function getCruftCmd(opts) {
        //We reverse sort here so that the moray lines come first, followed
        // by the mako lines.  The other way was to insert a useless field
        // into the map output.

        // Output is (currently):
        // [object uuid] [mako node] mako [size] [create time]
        // The demux will split into [job]-[mako node]-[uuid]
        return (getEnvCommon(opts) + ' \
export UUID=$(uuid) && \
export MANTA_PRE=/$MANTA_USER/stor/$MANTA_CRUFT/do && \
export MANTA_PATTERN=$MANTA_PRE/$MARLIN_JOB-{2}-$UUID && \
sort -r | ./build/node/bin/node ./bin/cruft.js | \
  ./build/node/bin/node ./bin/mdemux.js -p $MANTA_PATTERN \
');
}
/* END JSSTYLED */


function parseOptions() {
        var option;
        //First take what's in the config file, override what's on the
        // command line, and use the defaults if all else fails.
        var opts = MOLA_CRUFT_CONFIG_OBJ;
        opts.shards = opts.shards || [];
        var parser = new getopt.BasicParser('a:c:d:m:np:r:s:tx:', process.argv);

        while ((option = parser.getopt()) !== undefined) {
                if (option.error) {
                        usage();
                }

                switch (option.option) {
                case 'a':
                        opts.assetFile = option.optarg;
                        break;
                case 'c':
                        opts.cruftReducerCount = lib.common.parseNumberOption(
                            option.optarg, '-c', 1, null, usage);
                        break;
                case 'd':
                        opts.cruftReduceDisk = lib.common.parseNumberOption(
                            option.optarg, '-d', 1, null, usage);
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 'n':
                        opts.noJobStart = true;
                        break;
                case 'p':
                        opts.cruftMapDisk = lib.common.parseNumberOption(
                            option.optarg, '-p', 1, null, usage);
                        break;
                case 'r':
                        opts.cruftReduceMemory = lib.common.parseNumberOption(
                            option.optarg, '-r', 1, null, usage);
                        break;
                case 's':
                        opts.mantaStorageId = option.optarg;
                        break;
                case 't':
                        opts.jobName = 'manta_cruft_test';
                        opts.jobRoot = MP + '/manta_cruft_test';
                        break;
                case 'x':
                        opts.tooNewSeconds = lib.common.parseNumberOption(
                            option.optarg, '-x', 1, null, usage);
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        //Set up some defaults...
        opts.jobName = opts.jobName || 'manta_cruft';
        opts.jobRoot = opts.jobRoot || MP + '/manta_cruft';
        opts.assetDir = opts.jobRoot + '/assets';
        opts.assetObject = opts.assetDir + '/mola.tar.gz';
        opts.assetFile = opts.assetFile ||
                '/opt/smartdc/common/bundle/mola.tar.gz';

        opts.jobEnabled = opts.cruftEnabled;
        opts.cruftMapDisk = opts.cruftMapDisk || 32;
        opts.cruftReduceMemory = opts.cruftReduceMemory || 4096;
        opts.cruftReduceDisk = opts.cruftReduceDisk || 16;
        opts.marlinPathToAsset = opts.assetObject.substring(1);
        opts.marlinAssetObject = opts.assetObject;
        opts.tooNewSeconds = opts.tooNewSeconds || DEFAULT_TOO_NEW_SECONDS;

        opts.directories = [
                opts.jobRoot + '/do',
                opts.jobRoot + '/done'
        ];

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-a asset_object]';
        str += ' [-m moray_shard]';
        str += ' [-n no_job_start]';
        str += ' [-r marlin_reducer_memory]';
        str += ' [-s manta_storage_id]';
        str += ' [-t output_to_test]';
        console.error(str);
        process.exit(1);
}


function getCruftJob(opts, cb) {
        /*
         * There is at least one input file for each Moray shard and each Mako
         * storage zone.  Scale the number of reducers based on the number of
         * input files without exceeding the reducer count cap.  If needed,
         * this value can be overridden by setting the "CRUFT_REDUCER_COUNT"
         * property in the Manta SAPI application metadata, or by passing the
         * "-c" option to this program.
         */
        if (!opts.hasOwnProperty('cruftReducerCount')) {
                opts.cruftReducerCount = lib.common.reducerCurve(
                    opts.objects.length);
        }

        var pgCmd = getTransformCmd(opts);
        var cruftCmd = getCruftCmd(opts);

        var job = {
                phases: [ {
                        type: 'storage-map',
                        exec: pgCmd,
                        disk: opts.cruftMapDisk
                }, {
                        type: 'reduce',
                        count: opts.cruftReducerCount,
                        memory: opts.cruftReduceMemory,
                        disk: opts.cruftReduceDisk,
                        exec: cruftCmd
                } ]
        };

        LOG.info({ job: job }, 'Cruft Marlin Job Definition');

        cb(null, job);
}


function findObjects(opts, cb) {
        lib.common.findMorayMakoObjects({
                'client': MANTA_CLIENT,
                'log': LOG,
                'shards': opts.shards,
                'tablePrefixes': [ MANTA_DUMP_NAME_PREFIX,
                                   MANTA_DELETE_LOG_NAME_PREFIX ]
        }, function (err, res) {
                if (err) {
                        return (cb(err));
                }
                opts.earliestMakoDump = res.earliestMakoDump;
                opts.earliestMorayDump = res.earliestMorayDump;
                return (cb(null, res.objects));
        });
}



///--- Main

var _opts = parseOptions();

_opts.getJobDefinition = getCruftJob;
_opts.getJobObjects = findObjects;

var _doDir = _opts.jobRoot + '/do';
lib.common.getObjectsInDir({
        'client': MANTA_CLIENT,
        'dir': _doDir
}, function (err, objects) {
        if (err && err.name !== 'NotFoundError') {
                LOG.fatal(err, 'error fetching do objects');
                process.exit(1);
        }

        if (objects && objects.length > 0) {
                var m = 'Previous job output still exists in ' + _doDir +
                        '.  All previous output must be cleared before ' +
                        'a new job can be run.  Exiting...';
                LOG.info(m);
                process.exit(1);
        }

        var jobManager = lib.createJobManager(_opts, MANTA_CLIENT, LOG);
        jobManager.run(function () {
                MANTA_CLIENT.close();
                LOG.info('Done for now.');
        });
});
