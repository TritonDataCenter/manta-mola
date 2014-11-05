#!/usr/bin/env node
// -*- mode: js -*-
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var bunyan = require('bunyan');
var fs = require('fs');
var getopt = require('posix-getopt');
var lib = require('../lib');
var manta = require('manta');
var path = require('path');
var sprintf = require('sprintf-js').sprintf;



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

/* BEGIN JSSTYLED */
function getEnvCommon(opts) {
        return (' \
set -o pipefail && \
cd /assets/ && gtar -xzf ' + opts.marlinPathToAsset + ' && cd mola && \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getTransformCmd(opts) {
        var grepForStorageNode = '';
        if (opts.mantaStorageId) {
                grepForStorageNode = ' | grep ' + opts.mantaStorageId + ' | ';
        }
        return (getEnvCommon(opts) + ' \
gzcat -f | \
  ./build/node/bin/node ./bin/audit_transform.js -k $MANTA_INPUT_OBJECT \
    ' + grepForStorageNode + ' | \
  msplit -n ' + opts.numberReducers + ' \
');
}
/* END JSSTYLED */


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
        var parser = new getopt.BasicParser('a:d:m:np:r:s:t',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'a':
                        opts.assetFile = option.optarg;
                        break;
                case 'd':
                        opts.marlinReducerDisk = parseInt(option.optarg, 10);
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 'n':
                        opts.noJobStart = true;
                        break;
                case 'p':
                        opts.marlinMapDisk = parseInt(option.optarg, 10);
                        break;
                case 'r':
                        opts.marlinReducerMemory = parseInt(option.optarg, 10);
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

        opts.marlinMapDisk = opts.marlinMapDisk || 32;
        opts.marlinReducerMemory = opts.marlinReducerMemory || 4096;
        opts.marlinReducerDisk = opts.marlinReducerDisk || 16;
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
        //Use the same number of reducers as input files.
        opts.numberReducers = opts.objects.length;

        var pgCmd = getTransformCmd(opts);
        var auditCmd = getAuditCmd(opts);

        var job = {
                phases: [ {
                        type: 'storage-map',
                        exec: pgCmd,
                        disk: opts.marlinMapDisk
                }, {
                        type: 'reduce',
                        count: opts.numberReducers,
                        memory: opts.marlinReducerMemory,
                        disk: opts.marlinReducerDisk,
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

                gopts.path = parts[0];
                lib.common.getObject(parts[0], function (err2, errorLines) {
                        if (err2) {
                                //Don't know if it failed or not.
                                return (cb(err2));
                        }
                        if (errorLines !== '') {
                                //Bad juju.
                                LOG.fatal({
                                        job: job,
                                        outputObject: parts[0]
                                }, 'Audit job detected abnormalities between ' +
                                          'mako and moray.');
                        }
                        return (cb(null));
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
