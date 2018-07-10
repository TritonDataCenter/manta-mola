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
var verror = require('verror');

var VE = verror.VError;



///--- Global Objects

var NAME = 'mola-audit';
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: NAME,
        stream: process.stdout
});
var MOLA_AUDIT_CONFIG = (process.env.MOLA_AUDIT_CONFIG ||
                   '/opt/smartdc/mola/etc/config.json');
var MOLA_AUDIT_CONFIG_OBJ = JSON.parse(fs.readFileSync(MOLA_AUDIT_CONFIG));
var MANTA_CLIENT = manta.createClientFromFileSync(MOLA_AUDIT_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;



///--- Global Constants

var MP = '/' + MANTA_USER + '/stor';
var MANTA_DUMP_NAME_PREFIX = 'manta-';



///--- Helpers

function getEnvCommon(opts) {
        assert.string(opts.marlinPathToAsset, 'opts.marlinPathToAsset');

/* BEGIN JSSTYLED */
        return (' \
set -o pipefail && \
cd /assets/ && gtar -xzf ' + opts.marlinPathToAsset + ' && cd mola && \
');
/* END JSSTYLED */
}


function getTransformCmd(opts) {
        assert.number(opts.auditReducerCount, 'opts.auditReducerCount');

        var grepForStorageNode = '';
        if (opts.mantaStorageId) {
                grepForStorageNode = ' | grep ' + opts.mantaStorageId + ' | ';
        }

/* BEGIN JSSTYLED */
        return (getEnvCommon(opts) + ' \
gzcat -f | \
  ./build/node/bin/node ./bin/audit_transform.js -k $MANTA_INPUT_OBJECT \
    ' + grepForStorageNode + ' | \
  msplit -n ' + opts.auditReducerCount + ' \
');
/* END JSSTYLED */
}


/* BEGIN JSSTYLED */
function getAuditCmd(opts) {
        return (getEnvCommon(opts) + ' \
sort | ./build/node/bin/node ./bin/audit.js \
');
}
/* END JSSTYLED */


function parseOptions() {
        var option;
        //First take what's in the config file, override what's on the
        // command line, and use the defaults if all else fails.
        var opts = MOLA_AUDIT_CONFIG_OBJ;
        opts.shards = opts.shards || [];
        var parser = new getopt.BasicParser('a:c:d:m:np:r:s:t', process.argv);

        while ((option = parser.getopt()) !== undefined) {
                if (option.error) {
                        usage();
                }

                switch (option.option) {
                case 'a':
                        opts.assetFile = option.optarg;
                        break;
                case 'c':
                        opts.auditReducerCount = lib.common.parseNumberOption(
                            option.optarg, '-c', 1, null, usage);
                        break;
                case 'd':
                        opts.auditReduceDisk = lib.common.parseNumberOption(
                            option.optarg, '-d', 1, null, usage);
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 'n':
                        opts.noJobStart = true;
                        break;
                case 'p':
                        opts.auditMapDisk = lib.common.parseNumberOption(
                            option.optarg, '-p', 1, null, usage);
                        break;
                case 'r':
                        opts.auditReduceMemory = lib.common.parseNumberOption(
                            option.optarg, '-r', 1, null, usage);
                        break;
                case 's':
                        opts.mantaStorageId = option.optarg;
                        break;
                case 't':
                        opts.jobName = 'manta_audit_test';
                        opts.jobRoot = MP + '/manta_audit_test';
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        //Set up some defaults...
        opts.jobName = opts.jobName || 'manta_audit';
        opts.jobRoot = opts.jobRoot || MP + '/manta_audit';
        opts.assetDir = opts.jobRoot + '/assets';
        opts.assetObject = opts.assetDir + '/mola.tar.gz';
        opts.assetFile = opts.assetFile ||
                '/opt/smartdc/common/bundle/mola.tar.gz';

        opts.jobEnabled = opts.auditEnabled;
        opts.auditMapDisk = opts.auditMapDisk || 32;
        opts.auditReduceMemory = opts.auditReduceMemory || 4096;
        opts.auditReduceDisk = opts.auditReduceDisk || 16;
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
        str += ' [-m moray_shard]';
        str += ' [-n no_job_start]';
        str += ' [-r marlin_reducer_memory]';
        str += ' [-s manta_storage_id]';
        str += ' [-t output_to_test]';
        console.error(str);
        process.exit(1);
}


function getAuditJob(opts, cb) {
        /*
         * There is at least one input file for each Moray shard and each Mako
         * storage zone.  Scale the number of reducers based on the number of
         * input files without exceeding the reducer count cap.  If needed,
         * this value can be overridden by setting the "AUDIT_REDUCER_COUNT"
         * property in the Manta SAPI application metadata, or by passing the
         * "-c" option to this program.
         */
        if (!opts.hasOwnProperty('auditReducerCount')) {
                opts.auditReducerCount = lib.common.reducerCurve(
                    opts.objects.length);
        }

        var pgCmd = getTransformCmd(opts);
        var auditCmd = getAuditCmd(opts);

        var job = {
                phases: [ {
                        type: 'storage-map',
                        exec: pgCmd,
                        disk: opts.auditMapDisk
                }, {
                        type: 'reduce',
                        count: opts.auditReducerCount,
                        memory: opts.auditReduceMemory,
                        disk: opts.auditReduceDisk,
                        exec: auditCmd
                }, {
                        type: 'reduce',
                        count: 1,
                        exec: 'cat'
                } ]
        };

        LOG.info({ job: job }, 'Audit Marlin Job Definition');

        cb(null, job);
}


function findObjects(opts, cb) {
        lib.common.findMorayMakoObjects({
                'client': MANTA_CLIENT,
                'log': LOG,
                'shards': opts.shards,
                'tablePrefixes': [ MANTA_DUMP_NAME_PREFIX ]
        }, function (err, res) {
                if (err) {
                        return (cb(err));
                }
                return (cb(null, res.objects));
        });
}


function checkJobResults(job, audit, opts, cb) {
        // If the job was cancelled, we don't want any alarms.
        if (job.cancelled) {
                return (cb(null));
        }

        if (job.stats.errors > 0) {
                //Log an explicit error to fire an alarm.
                LOG.error({ jobId: job.id }, 'audit job had errors');
                return (cb(null));
        }

        //Find the output
        var gopts = {
                'client': MANTA_CLIENT,
                'path': '/' + MANTA_CLIENT.user + '/jobs/' + job.id +
                        '/out.txt'
        };
        lib.common.getObject(gopts, function (err, res) {
                if (err) {
                        //Don't know if it failed or not, so don't audit.
                        return (cb(err));
                }

                LOG.info({ jobId: job.id, outputs: res },
                         'Looking at job output.');
                var parts = res.split('\n');
                if (parts.length !== 2 && parts[1] !== '') {
                        LOG.fatal({ jobId: job.id },
                                  'Job doesn\'t have one output!');
                        return (cb(null));
                }

                /*
                 * In some cases, the output from an audit job can be quite
                 * large.  We don't need to inspect the contents here, just
                 * report on whether or not the object contained any output.
                 */
                var auditOutput = parts[0];
                MANTA_CLIENT.info(auditOutput, function (infoErr, info) {
                        if (infoErr) {
                                cb(VE(infoErr, 'checking audit output "%s"',
                                    auditOutput));
                                return;
                        }

                        if (typeof (info.size) !== 'number') {
                                cb(VE('invalid "size" for audit output "%s"',
                                    auditOutput));
                                return;
                        }

                        if (info.size === 0) {
                                /*
                                 * The output object is empty, meaning the
                                 * audit job did not report any problems.
                                 */
                                cb(null);
                                return;
                        }

                        /*
                         * Emit a log message at the FATAL level to trigger
                         * the appropriate alarm.
                         */
                        LOG.fatal({ job: job, outputObject: auditOutput,
                            outputObjectInfo: info }, 'Audit job detected ' +
                            'abnormalities between Mako and Moray.');
                        cb(null);
                });
        });
}


///--- Main

var _opts = parseOptions();

_opts.getJobDefinition = getAuditJob;
_opts.getJobObjects = findObjects;
_opts.preAudit = checkJobResults;

var jobManager = lib.createJobManager(_opts, MANTA_CLIENT, LOG);
jobManager.run(function () {
        MANTA_CLIENT.close();
        LOG.info('Done for now.');
});
