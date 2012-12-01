#!/usr/bin/env node
// -*- mode: js -*-
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var getopt = require('posix-getopt');
var exec = require('child_process').exec;
var lib = require('../lib');
var manta = require('manta');
var path = require('path');
var sys = require('sys');
var vasync = require('vasync');



///--- Global Objects

var NAME = 'mola';
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: NAME,
        stream: process.stdout
});
var MANTA_CONFIG = (process.env.MANTA_CONFIG ||
                    '/opt/smartdc/common/etc/config.json');
var MANTA_CLIENT = manta.createClientFromFileSync(MANTA_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;



///--- Global Strings

var MP = '/' + MANTA_USER + '/stor';
var BACKUP_DIR = MP + '/manatee_backups';
var MOLA_CODE_BUNDLE = (process.env.MOLA_CODE_BUNDLE ||
                        '/opt/smartdc/common/bundle/mola.tar.gz');
var GC_JOB_NAME = 'manta_gc';
var MANTA_GC_DIR = MP + '/manta_gc';
var MANTA_ASSET_DIR = MANTA_GC_DIR + '/assets';
var MOLA_ASSET_KEY = MANTA_ASSET_DIR + '/mola.tar.gz';
var MANTA_DUMP_NAME_PREFIX = 'manta-';
var MANTA_DELETE_LOG_DUMP_NAME_PREFIX = 'manta_delete_log-';
var RUNNING_STATE = 'running';

//In Marlin
var MARLIN_PATH_TO_ASSET = MOLA_ASSET_KEY.substring(1);
var MARLIN_ASSET_KEY = MOLA_ASSET_KEY;
var MANTA_GC_ALL_DIR = MANTA_GC_DIR + '/all';
var MANTA_GC_ADO_DIR = MANTA_GC_DIR + '/all/do';
var MANTA_GC_ADN_DIR = MANTA_GC_DIR + '/all/done';
var MANTA_GC_MAKO_DIR = MANTA_GC_DIR + '/mako';
var MANTA_GC_MORAY_DIR = MANTA_GC_DIR + '/moray';



///--- Marlin Commands

/* BEGIN JSSTYLED */
//TODO: Delete me
var WHILE_1 = 'env >/tmp/env.txt && \
while [[ true ]]; do echo Hello; sleep 2; done';
var ECHO_HELLO = 'echo "Hello World!"';

var ENV_COMMON = 'export PATH=/usr/node/bin:$PATH && \
export MANTA_USER=' + MANTA_USER + ' && \
export MANTA_GC=' + GC_JOB_NAME + ' && \
export MARLIN_JOB=$(echo $MANTA_OUTPUT_BASE | cut -d "/" -f 4) && \
export NOW=$(date "+%Y-%m-%d-%H-%M-%S") && \
cd /assets/ && tar -xzf ' + MARLIN_PATH_TO_ASSET + ' && cd mola && \
';
/* END JSSTYLED */



///--- Helpers

/* BEGIN JSSTYLED */
function getPgTransformCmd(earliestDumpDate, nReducers) {
        return (ENV_COMMON + ' \
export MORAY_SHARD=$(echo $mc_input_key | cut -d "/" -f 5) && \
export DUMP_DATE=$(echo $mc_input_key | cut -d "/" -f 6) && \
zcat | \
  node ./bin/pg_transform.js -d $DUMP_DATE -e ' + earliestDumpDate + ' \
    -m $MORAY_SHARD | \
  msplit -n ' + nReducers + ' \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getGcCmd(gracePeriodSeconds) {
        var gracePeriodOption = '';
        if (gracePeriodSeconds) {
                gracePeriodOption = ' -g ' + gracePeriodSeconds;
        }
        //We use a UUID only because there's no way (yet) to get a reference
        // to which reducer this is running on.
        return (ENV_COMMON + ' \
export UUID=$(uuid) && \
export MANTA_PRE=/$MANTA_USER/stor/$MANTA_GC/all && \
export MANTA_FILE_PRE=$MANTA_PRE/done/$NOW-$MARLIN_JOB-X-$UUID && \
export MANTA_PATTERN=$MANTA_FILE_PRE-{1}-{2} && \
export MANTA_LINKS=$MANTA_PRE/do/$NOW-$MARLIN_JOB-X-$UUID-links && \
export PERL=/usr/perl5/bin/perl && \
export LINKS_FILE=./links.txt && \
sort | node ./bin/gc.js' + gracePeriodOption + ' | \
  $PERL ./bin/gc_links.pl $MANTA_USER $LINKS_FILE $MANTA_FILE_PRE | \
  node ./bin/mdemux.js -p $MANTA_PATTERN && \
cat $LINKS_FILE | mpipe $MANTA_LINKS \
');
}
/* END JSSTYLED */


function parseOptions() {
        var option;
        var opts = {};
        opts.shards = [];
        var parser = new getopt.BasicParser('g:m:',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'g':
                        opts.gracePeriodSeconds = parseInt(option.optarg, 10);
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }
        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-g grace_period_seconds]';
        console.error(str);
        process.exit(1);
}


function ifError(err, msg) {
        if (err) {
                LOG.error(err, msg);
                process.exit(1);
        }
}


function startsWith(str, prefix) {
        return (str.slice(0, prefix.length) === prefix);
}


function endsWith(str, suffix) {
        return (str.indexOf(suffix, str.length - suffix.length) !== -1);
}


function getObjectsInDir(dir, cb) {
        var keys = [];
        MANTA_CLIENT.ls(dir, {}, function (err, res) {
                ifError(err);

                res.on('object', function (obj) {
                        keys.push(dir + '/' + obj.name);
                });

                res.once('error', function (err2) {
                        cb(err2);
                });

                res.once('end', function () {
                        cb(null, keys);
                });
        });
}


function findLatestBackupObjects(opts, cb) {
        if ((typeof (opts)) === 'string' || opts instanceof String) {
                opts = {
                        dir: BACKUP_DIR + '/' + opts
                };
        }
        assert.string(opts.dir);

        var dir = opts.dir;

        MANTA_CLIENT.ls(dir, {}, function (err, res) {
                ifError(err);

                var dirs = [];
                var objs = [];

                res.on('directory', function (d) {
                        dirs.push(d.name);
                });

                res.on('object', function (o) {
                        objs.push(o.name);
                });

                res.on('error', function (err2) {
                        cb(err2);
                });

                res.on('end', function () {
                        //Assume that if there's objects or no further
                        // directories to walk down, we're done.
                        if (dirs.length === 0 || objs.length > 0) {
                                cb(null, {
                                        directory: dir,
                                        objects: objs
                                });
                                return;
                        }
                        dirs.sort(function (a, b) { return (b - a); });
                        dir += '/' + dirs[0];
                        findLatestBackupObjects({ dir: dir }, cb);
                });
        });
}


function createGcMarlinJob(opts) {
        //We use the number of shards + 1 so that we know
        // we are always using multiple reducers.  There's
        // no reason this can't be much more.
        var nReducers = opts.shards.length + 1;
        var pgCmd = getPgTransformCmd(opts.earliest_dump, nReducers);
        var gcCmd = getGcCmd(opts.gracePeriodSeconds);
        var job = {
                name: GC_JOB_NAME,
                phases: [ {
                        type: 'storage-map',
                        assets: [ MARLIN_ASSET_KEY ],
                        exec: pgCmd
                }, {
                        type: 'reduce',
                        count: nReducers,
                        assets: [ MARLIN_ASSET_KEY ],
                        exec: gcCmd
                } ]
        };

        LOG.info({ job: job }, 'GC Marlin Job Definition');

        MANTA_CLIENT.createJob(job, function (err, jobId) {
                ifError(err);

                LOG.info({ jobId: jobId }, 'Created Job.');
                var aopts = {
                        end: true
                };
                var keys = opts.keys;

                //Add keys to job...
                MANTA_CLIENT.addJobKey(jobId, keys, aopts, function (err2) {
                        ifError(err2);

                        LOG.info({
                                keys: keys,
                                jobId: jobId
                        }, 'Added keys to job');
                        LOG.info('Done for now.');
                });
        });
}


function setupGcMarlinJob(opts) {
        //Make sure the right directories have been created...
        var m = MANTA_CLIENT;
        vasync.pipeline({
                funcs: [
                        function (_, cb) { m.mkdir(MANTA_GC_DIR, cb); },
                        function (_, cb) { m.mkdir(MANTA_ASSET_DIR, cb); },
                        function (_, cb) { m.mkdir(MANTA_GC_ALL_DIR, cb); },
                        function (_, cb) { m.mkdir(MANTA_GC_ADO_DIR, cb); },
                        function (_, cb) { m.mkdir(MANTA_GC_ADN_DIR, cb); },
                        function (_, cb) { m.mkdir(MANTA_GC_MAKO_DIR, cb); },
                        function (_, cb) { m.mkdir(MANTA_GC_MORAY_DIR, cb); }
                ]
        }, function (err) {
                ifError(err);

                //Upload the bundle to manta
                fs.stat(MOLA_CODE_BUNDLE, function (err2, stats) {
                        ifError(err2);

                        if (!stats.isFile()) {
                                LOG.error(MOLA_CODE_BUNDLE +
                                          ' isnt a file');
                                process.exit(1);
                        }

                        var o = {
                                copies: 2,
                                size: stats.size
                        };

                        var s = fs.createReadStream(MOLA_CODE_BUNDLE);
                        var p = MOLA_ASSET_KEY;
                        s.pause();
                        s.on('open', function () {
                                MANTA_CLIENT.put(p, s, o, function (e) {
                                        ifError(e);
                                        createGcMarlinJob(opts);
                                });
                        });
                });
        });
}


//Expects the filename to be in the format:
// manta-2012-11-30-23-00-07.gz
function extractDate(prefix, filename) {
        var d = filename.replace(prefix, '');
        d = d.substring(0, d.indexOf('.'));
        return (d);
}


function runGcWithShards(opts) {
        LOG.info({ opts: opts }, 'Running GC with shards.');
        var shards = opts.shards;
        vasync.forEachParallel({
                func: findLatestBackupObjects,
                inputs: shards
        }, function (err, results) {
                ifError(err);
                if (results.successes.length !== shards.length) {
                        LOG.error('Couldnt find latest backup for all shards.');
                        process.exit(1);
                }

                var keys = [];
                var dates = [];

                for (var i = 0; i < shards.length; ++i) {
                        var res = results.successes[i];
                        var dir = res.directory;
                        var objs = res.objects;

                        //Search the objects for the tables we need to process
                        var foundManta = false;
                        var foundMantaDeleteLog = false;
                        var mdnp = MANTA_DUMP_NAME_PREFIX;
                        var mdldnp = MANTA_DELETE_LOG_DUMP_NAME_PREFIX;
                        for (var j = 0; j < objs.length; ++j) {
                                var obj = objs[j];
                                if (startsWith(obj, mdnp)) {
                                        foundManta = true;
                                        keys.push(dir + '/' + obj);
                                        //Get the date from the filename...
                                        dates.push(extractDate(mdnp, obj));
                                } else if (startsWith(obj, mdldnp)) {
                                        foundMantaDeleteLog = true;
                                        keys.push(dir + '/' + obj);
                                }
                        }

                        if (!foundManta || !foundMantaDeleteLog) {
                                LOG.error({ dir: dir, objs: objs },
                                          'Couldnt find all tables in dump ' +
                                          'directory.');
                                process.exit(1);
                        }
                }

                dates.sort();
                LOG.info({ dates: dates }, 'found dates');
                var earliest_dump = dates[0];

                opts.keys = keys;
                opts.earliest_dump = earliest_dump;
                setupGcMarlinJob(opts);
        });
}

function verifyShardsAndContinue(mantaShards, mdataShards, opts) {
        if (mantaShards.length !== mdataShards.length) {
                LOG.fatal({
                        mantaShards: mantaShards,
                        mdataShards: mdataShards,
                        opts: opts
                }, 'shard lists in manta and mdata (or cli) dont match.');
                process.exit(1);
        }
        mantaShards.sort();
        mdataShards.sort();
        for (var i = 0; i < mantaShards.length; ++i) {
                if (mantaShards[i] !== mdataShards[i]) {
                        LOG.fatal({
                                mantaShards: mantaShards,
                                mdataShards: mdataShards,
                                opts: opts
                        }, 'shard lists in manta and mdata (or cli) dont ' +
                                  'match.');
                        process.exit(1);
                }
        }
        opts.shards = mantaShards;
        LOG.info({ opts: opts }, 'Running with shards.');
        runGcWithShards(opts);
}

function getShardsFromManta(cb) {
        MANTA_CLIENT.ls(BACKUP_DIR, {}, function (err, res) {
                ifError(err);

                var shards = [];

                res.on('directory', function (dir) {
                        shards.push(dir.name);
                });

                res.on('error', function (err2) {
                        if (err2.code === 'ResourceNotFound') {
                                LOG.info(BACKUP_DIR + ' doesnt exist.');
                                process.exit(0);
                        }
                        ifError(err2);
                });

                res.on('end', function () {
                        cb(null, shards);
                });
        });
}


function getShardsFromMdata(cb) {
        var cmd = 'mdata-get moray_indexer_names';
        LOG.info({ cmd: cmd }, 'fetching data from mdata');
        exec(cmd, function (err, stdout, stderr) {
                ifError(err, 'fetching from mdata failed.');
                var shards = stdout.split(/\s+/);
                while(shards[shards.length - 1] === '') {
                        shards.pop();
                }
                cb(null, shards);
        });
}


function findShards(opts) {
        getShardsFromManta(function (err, mantaShards) {
                ifError(err);

                if (mantaShards.length === 0) {
                        LOG.info('no moray shards found in manta.');
                        process.exit(0);
                }

                //This means the one running the command is responsible for
                // giving the correct set of shards...
                if (opts.shards.length > 0) {
                        verifyShardsAndContinue(mantaShards, opts.shards, opts);
                } else {
                        getShardsFromMdata(function (err2, mdataShards) {
                                ifError(err2);

                                if (mantaShards.length === 0) {
                                        var m = 'no moray shards found in ' +
                                                'mdata.'
                                        LOG.info(m);
                                        process.exit(0);
                                }
                                verifyShardsAndContinue(mantaShards,
                                                        mdataShards,
                                                        opts);
                        });
                }
        });
}



///--- Main

var _opts = parseOptions();

//If a job is already running, kick out.
MANTA_CLIENT.listJobs({ state: RUNNING_STATE }, function (err, res) {
        ifError(err);

        var gcRunning = false;
        var gcObject = {};

        res.on('job', function (job) {
                if (job.name.indexOf(GC_JOB_NAME) === 0) {
                        gcRunning = true;
                        gcObject = job;
                }
        });

        res.on('error', function (err3) {
                LOG.error(err3);
                process.exit(1);
        });

        res.on('end', function () {
                if (gcRunning) {
                        LOG.info(gcObject, 'GC Job already running.');
                        process.exit(1);
                }
                findShards(_opts);
        });
});
