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
var MANTA_DUMP_NAME_PREFIX = 'manta-';
var MANTA_DELETE_LOG_DUMP_NAME_PREFIX = 'manta_delete_log-';
var RUNNING_STATE = 'running';



///--- Helpers

/* BEGIN JSSTYLED */
function getEnvCommon(opts) {
        return (' \
export MANTA_USER=' + MANTA_USER + ' && \
export MANTA_GC=' + opts.gcJobName + ' && \
export MARLIN_JOB=$(echo $MANTA_OUTPUT_BASE | cut -d "/" -f 4) && \
export NOW=$(date "+%Y-%m-%d-%H-%M-%S") && \
cd /assets/ && gtar -xzf ' + opts.marlinPathToAsset + ' && cd mola && \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getPgTransformCmd(opts) {
        var grepForObject = '';
        if (opts.objectId) {
                grepForObject = ' grep ' + opts.objectId + ' |';
        }
        return (getEnvCommon(opts) + ' \
export MORAY_SHARD=$(echo $mc_input_key | cut -d "/" -f 5) && \
export DUMP_DATE=$(basename $mc_input_key | sed \'s/^\\w*-//; s/.gz$//;\') && \
zcat | \
  ./build/node/bin/node ./bin/pg_transform.js -d $DUMP_DATE \
    -e ' + opts.earliestDumpDate + ' \
    -m $MORAY_SHARD |' + grepForObject + ' \
  msplit -n ' + opts.numberReducers + ' \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getGcCmd(opts) {
        var gracePeriodOption = '';
        if (opts.gracePeriodSeconds) {
                gracePeriodOption = ' -g ' + opts.gracePeriodSeconds;
        }
        //We use a UUID only because there's no way (yet) to get a reference
        // to which reducer this is running on.
        return (getEnvCommon(opts) + ' \
export UUID=$(uuid) && \
export MANTA_PRE=/$MANTA_USER/stor/$MANTA_GC/all && \
export MANTA_FILE_PRE=$MANTA_PRE/done/$NOW-$MARLIN_JOB-X-$UUID && \
export MANTA_PATTERN=$MANTA_FILE_PRE-{1}-{2} && \
export MANTA_LINKS=$MANTA_PRE/do/$NOW-$MARLIN_JOB-X-$UUID-links && \
export PERL=/usr/perl5/bin/perl && \
export LINKS_FILE=./links.txt && \
sort | ./build/node/bin/node ./bin/gc.js' + gracePeriodOption + ' | \
  $PERL ./bin/gc_links.pl $MANTA_USER $LINKS_FILE $MANTA_FILE_PRE | \
  ./build/node/bin/node ./bin/mdemux.js -p $MANTA_PATTERN && \
cat $LINKS_FILE | mpipe $MANTA_LINKS \
');
}
/* END JSSTYLED */


function parseOptions() {
        var option;
        var opts = {};
        opts.shards = [];
        var parser = new getopt.BasicParser('g:m:o:r:t',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'g':
                        opts.gracePeriodSeconds = parseInt(option.optarg, 10);
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 'o':
                        opts.objectId = option.optarg;
                        break;
                case 'r':
                        opts.marlinReducerMemory = parseInt(option.optarg, 10);
                        break;
                case 't':
                        opts.gcJobName = 'manta_gc_test';
                        opts.mantaGcDir = MP + '/manta_gc_test';
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        //Set up some defaults...
        opts.gcJobName = opts.gcJobName || 'manta_gc';
        opts.mantaGcDir = opts.mantaGcDir || MP + '/manta_gc';
        opts.mantaAssetDir = opts.mantaGcDir + '/assets';
        opts.molaAssetKey = opts.mantaAssetDir + '/mola.tar.gz';

        opts.marlinReducerMemory = opts.marlinReducerMemory || 4096;
        opts.marlinPathToAsset = opts.molaAssetKey.substring(1);
        opts.marlinAssetKey = opts.molaAssetKey;

        opts.mantaGcAllDir = opts.mantaGcDir + '/all';
        opts.mantaGcAdoDir = opts.mantaGcDir + '/all/do';
        opts.mantaGcAdoneDir = opts.mantaGcDir + '/all/done';
        opts.mantaGcMakoDir = opts.mantaGcDir + '/mako';
        opts.mantaGcMorayDir = opts.mantaGcDir + '/moray';

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-g grace_period_seconds]';
        str += ' [-m moray_shard]';
        str += ' [-o object_id]';
        str += ' [-r marlin_reducer_memory]';
        str += ' [-t output_to_test]';
        console.error(str);
        process.exit(1);
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
                if (err) {
                        cb(err);
                        return;
                }

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
                if (err) {
                        cb(err);
                        return;
                }

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


function createGcMarlinJob(opts, cb) {
        //We use the number of shards + 1 so that we know
        // we are always using multiple reducers.  There's
        // no reason this can't be much more.

        //MANTA-840
        //var nReducers = opts.shards.length + 1;
        opts.numberReducers = 1;

        var pgCmd = getPgTransformCmd(opts);
        var gcCmd = getGcCmd(opts);
        var job = {
                name: opts.gcJobName,
                phases: [ {
                        type: 'storage-map',
                        assets: [ opts.marlinAssetKey ],
                        exec: pgCmd
                }, {
                        type: 'reduce',
                        count: opts.numberReducers,
                        assets: [ opts.marlinAssetKey ],
                        memory: opts.marlinReducerMemory,
                        exec: gcCmd
                } ]
        };

        LOG.info({ job: job }, 'GC Marlin Job Definition');

        MANTA_CLIENT.createJob(job, function (err, jobId) {
                if (err) {
                        cb(err);
                        return;
                }

                LOG.info({ jobId: jobId }, 'Created Job.');
                var aopts = {
                        end: true
                };
                var keys = opts.keys;

                //Add keys to job...
                MANTA_CLIENT.addJobKey(jobId, keys, aopts, function (err2) {
                        if (err2) {
                                cb(err2);
                                return;
                        }

                        LOG.info({
                                keys: keys,
                                jobId: jobId
                        }, 'Added keys to job');
                        LOG.info('Done for now.');
                        cb();
                });
        });
}


function setupGcDirectories(opts, cb) {
        var m = MANTA_CLIENT;
        vasync.pipeline({
                funcs: [
                        function (_, c) { m.mkdir(opts.mantaGcDir, c); },
                        function (_, c) { m.mkdir(opts.mantaAssetDir, c); },
                        function (_, c) { m.mkdir(opts.mantaGcAllDir, c); },
                        function (_, c) { m.mkdir(opts.mantaGcAdoDir, c); },
                        function (_, c) { m.mkdir(opts.mantaGcAdoneDir, c); },
                        function (_, c) { m.mkdir(opts.mantaGcMakoDir, c); },
                        function (_, c) { m.mkdir(opts.mantaGcMorayDir, c); }
                ]
        }, function (err) {
                cb(err);
        });
}


function setupGcMarlinJob(opts, cb) {
        setupGcDirectories(opts, function (err) {
                if (err) {
                        cb(err);
                        return;
                }

                //Upload the bundle to manta
                fs.stat(MOLA_CODE_BUNDLE, function (err2, stats) {
                        if (err2) {
                                cb(err2);
                                return;
                        }

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
                        var p = opts.molaAssetKey;
                        s.pause();
                        s.on('open', function () {
                                MANTA_CLIENT.put(p, s, o, function (e) {
                                        if (e) {
                                                cb(e);
                                                return;
                                        }
                                        createGcMarlinJob(opts, cb);
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


function runGcWithShards(opts, cb) {
        LOG.info({ opts: opts }, 'Running GC with shards.');
        var shards = opts.shards;
        vasync.forEachParallel({
                func: findLatestBackupObjects,
                inputs: shards
        }, function (err, results) {
                if (err) {
                        cb(err);
                        return;
                }
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
                opts.earliestDumpDate = dates[0];
                opts.keys = keys;
                setupGcMarlinJob(opts, cb);
        });
}


function getShardsFromMdata(cb) {
        var cmd = 'mdata-get moray_indexer_names';
        LOG.info({ cmd: cmd }, 'fetching data from mdata');
        exec(cmd, function (err, stdout, stderr) {
                if (err) {
                        cb(err);
                        return;
                }
                var shards = stdout.split(/\s+/);
                while (shards[shards.length - 1] === '') {
                        shards.pop();
                }
                cb(null, shards);
        });
}


function findShards(opts, cb) {
        //This means the one running the command is responsible for
        // giving the correct set of shards...
        if (opts.shards && opts.shards.length > 0) {
                runGcWithShards(opts, cb);
        } else {
                getShardsFromMdata(function (err2, mdataShards) {
                        if (err2) {
                                cb(err2);
                                return;
                        }

                        if (mdataShards.length === 0) {
                                var m = 'no moray shards found in mdata.';
                                LOG.info(m);
                                process.exit(0);
                        }
                        opts.shards = mdataShards;
                        runGcWithShards(opts, cb);
                });
        }
}


function findRunningGcJobs(opts, cb) {
        var lopts = { state: RUNNING_STATE };
        MANTA_CLIENT.listJobs(lopts, function (err, res) {
                if (err) {
                        cb(err);
                        return;
                }

                var gcObject = null;

                res.on('job', function (job) {
                        if (job.name.indexOf(opts.gcJobName) === 0) {
                                gcObject = job;
                        }
                });

                res.on('error', function (err3) {
                        cb(err3);
                });

                res.on('end', function () {
                        cb(null, gcObject);
                });
        });
}


function checkForRunningJobs(opts, cb) {
        findRunningGcJobs(opts, function (err, job) {
                if (err) {
                        cb(err);
                        return;
                }

                if (job) {
                        LOG.info(job, 'GC Job already running.');
                        cb();
                        return;
                }

                findShards(opts, cb);
        });
}



///--- Main

var _opts = parseOptions();

checkForRunningJobs(_opts, function (err) {
        if (err) {
                LOG.fatal(err, 'Error.');
        }
        //TODO: Write out audit record.
        process.exit(0);
});
