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

var assert = require('assert-plus');
var bunyan = require('bunyan');
var carrier = require('carrier');
var fs = require('fs');
var getopt = require('posix-getopt');
var manta = require('manta');
var moray = require('moray');
var path = require('path');
var vasync = require('vasync');



///--- Globals

var NAME = 'cruft_verify_and_link';
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: NAME,
        stream: process.stdout
});
var MOLA_CONFIG = (process.env.MOLA_CONFIG ||
                    '/opt/smartdc/mola/etc/config.json');
var MANTA_CLIENT = manta.createClientFromFileSync(MOLA_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;
var MANTA_DO_DIR = '/' + MANTA_USER + '/stor/manta_cruft/do';
var MANTA_DONE_DIR = '/' + MANTA_USER + '/stor/manta_cruft/done';
var MANTA_GC_PREFIX = '/' + MANTA_USER + '/stor/manta_gc/mako';
var AUDIT = {
        'audit': true,
        'cronExec': 1,
        'cronFailed': 1,
        'count': 0,
        'startTime': new Date()
};

var MORAY_BUCKETS = ['manta', 'manta_delete_log'];
var MORAY_CONNECT_TIMEOUT = 1000;
var MORAY_PORT = 2020;
var MAX_OBJECTS_TO_CHECK = 100;
var TMP_DIR = '/var/tmp/manta_cruft';


///--- Helpers

function parseOptions() {
        var option;
        var jsonOpts = fs.readFileSync(MOLA_CONFIG);
        var opts = JSON.parse(jsonOpts);
        var parser = new getopt.BasicParser('L',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'L':
                        opts.noLink = true;
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
        str += ' [-L verify but don\'t link]';
        console.error(str);
        process.exit(1);
}


//TODO: Use the one in common
function deleteObject(objPath, cb) {
        LOG.info({ objPath: objPath }, 'deleting object');
        ++AUDIT.count;
        MANTA_CLIENT.unlink(objPath, function (err) {
                return (cb(err));
        });
}


//TODO: Use the one in common
function link(linkObj, cb) {
        LOG.info({ linkObj: linkObj }, 'linking object');
        MANTA_CLIENT.ln(linkObj.from, linkObj.to, function (err) {
                return (cb(err));
        });
}


//TODO: Use the one in common
function getObjectsInDir(dir, cb) {
        var objects = [];
        MANTA_CLIENT.ls(dir, {}, function (err, res) {
                if (err) {
                        cb(err);
                        return;
                }

                res.on('object', function (obj) {
                        objects.push(dir + '/' + obj.name);
                });

                res.once('error', function (err2) {
                        cb(err2);
                });

                res.once('end', function () {
                        cb(null, objects);
                });
        });
}


//TODO: Use the one in common
function getObjectToFile(opts, cb) {
        assert.object(opts, 'opts');
        assert.object(opts.client, 'opts.client');
        assert.string(opts.file, 'opts.file');
        assert.string(opts.path, 'opts.path');

        var out = fs.createWriteStream(opts.file);
        out.on('open', function () {
                opts.client.get(opts.path, function (err, stream, res) {
                        if (err) {
                                return (cb(err));
                        }

                        stream.pipe(out);
                        stream.on('end', cb);
                });
        });
}


function getMorayObjects(opts, cb) {
        assert.object(opts, 'opts');
        assert.string(opts.filter, 'opts.filter');
        assert.object(opts.client, 'opts.client');
        assert.string(opts.bucket, 'opts.bucket');

        var records = [];
        var ended = false;
        function end(e) {
                if (!ended) {
                        ended = true;
                        return (cb(e, records));
                }
        }

        var res = opts.client.findObjects(opts.bucket, opts.filter);
        res.on('error', end);
        res.on('end', end);
        res.on('record', function (r) {
                records.push(r);
        });
}


function checkForObjectsInMorays(opts, cb) {
        assert.object(opts, 'opts');
        assert.arrayOfString(opts.objects, 'opts.objects');
        assert.arrayOfObject(opts.morays, 'opts.morays');

        var filter = '(|';
        opts.objects.forEach(function (o) {
                filter += '(objectId=' + o + ')';
        });
        filter += ')';

        var inputs = [];
        opts.morays.forEach(function (m) {
                MORAY_BUCKETS.forEach(function (b) {
                        inputs.push({
                                'filter': filter,
                                'client': m,
                                'bucket': b
                        });
                });
        });

        vasync.forEachParallel({
                'func': getMorayObjects,
                'inputs': inputs
        }, function (err, res) {
                if (err) {
                        return (cb(err));
                }

                for (var i = 0; i < inputs.length; ++i) {
                        var result = res.operations[i].result;
                        if (result.length > 0) {
                                var oids = result.map(function (o) {
                                        return (o.value.objectId);
                                });
                                var m = 'found invalid cruft objects';
                                LOG.fatal({
                                        'objectIds': oids,
                                        'moray': inputs[i].client.host,
                                        'bucket': inputs[i].bucket
                                }, m);
                                return (cb(new Error(m)));
                        }
                }

                // Ok, that batch is good!
                return (cb());
        });
}


// Verifies objects in the file, returns the mako shard(s).
function verifyObjectsInFile(opts, cb) {
        assert.object(opts, 'opts');
        assert.string(opts.file, 'opts.file');
        assert.arrayOfObject(opts.morays, 'opts.morays');

        var closed = false;
        var error = null;
        var makos = [];
        var carry = carrier.carry(fs.createReadStream(opts.file));
        var queue = vasync.queue(function (_, subcb) {
                if (closed) {
                        return (subcb());
                }
                checkForObjectsInMorays(_, function (err) {
                        //Fail fast...
                        if (err) {
                                closed = true;
                                error = err;
                                queue.close();
                        }
                        return (subcb());
                });
        }, 1);

        queue.on('end', function () {
                return (cb(error, makos));
        });

        var objects = [];
        carry.on('line', function (line) {
                if (closed) {
                        return;
                }
                //Expecting: 'mako' mako owner object ...
                var parts = line.split('\t');
                if (makos.indexOf(parts[1]) === -1) {
                        makos.push(parts[1]);
                }
                objects.push(parts[3]);
                if (objects.length >= MAX_OBJECTS_TO_CHECK) {
                        queue.push({ 'objects': objects,
                                     'morays': opts.morays });
                        objects = [];
                }
        });

        carry.on('end', function () {
                if (closed) {
                        return;
                }
                if (objects.length > 0) {
                        queue.push({ 'objects': objects,
                                     'morays': opts.morays });
                }
                queue.close();
        });
}


function linkAndMoveFile(opts, cb) {
        assert.object(opts, 'opts');
        assert.arrayOfString(opts.makos, 'opts.makos');
        assert.string(opts.path, 'opts.path');
        assert.object(opts.client, 'opts.client');

        var filename = path.basename(opts.path);
        var doneLocation = MANTA_DONE_DIR + '/' + filename;
        var gcLocations = opts.makos.map(function (m) {
                return (MANTA_GC_PREFIX + '/' + m + '/CRUFT-' + filename);
        });

        LOG.info({
                'oldPath': opts.path,
                'makoGcLocations': gcLocations,
                'newPath': doneLocation
        }, 'linking and moving');

        var links = [].concat(gcLocations);
        links.push(doneLocation);

        vasync.forEachParallel({
                'func': function createLink(newLoc, subcb) {
                        opts.client.ln(opts.path, newLoc, subcb);
                },
                'inputs': links
        }, function (err) {
                if (err) {
                        return (cb(err));
                }

                opts.client.unlink(opts.path, cb);
        });
}


function verifyAndLinkFile(opts, cb) {
        //The other side of the wonky
        var cruft = opts.cruft;
        opts = opts.opts;

        LOG.info({
                'cruft': cruft
        }, 'starting verification');

        var filename = path.basename(cruft);
        var file = TMP_DIR + '/' + filename;
        var makos = null;
        vasync.pipeline({
                'funcs': [
                        function getToFile(_, subcb) {
                                LOG.info({
                                        'path': cruft,
                                        'file': file
                                }, 'downloading cruft file');
                                getObjectToFile({
                                        'client': MANTA_CLIENT,
                                        'file': file,
                                        'path': cruft
                                }, subcb);
                        },
                        function checkFile(_, subcb) {
                                LOG.info({
                                        'path': cruft,
                                        'file': file
                                }, 'Checking objects in file');
                                verifyObjectsInFile({
                                        'file': file,
                                        'morays': opts.morays
                                }, function (err, ms) {
                                        if (err) {
                                                return (subcb(err));
                                        }
                                        LOG.info({
                                                'path': cruft
                                        }, 'verified ok');
                                        makos = ms;
                                        return (subcb());
                                });
                        },
                        function rmTmpFile(_, subcb) {
                                fs.unlink(file, subcb);
                        },
                        function linkFiles(_, subcb) {
                                if (opts.noLink) {
                                        LOG.info({
                                                'path': cruft,
                                                'makos': makos
                                        }, 'NOT linking');
                                        return (subcb());
                                }
                                LOG.info({
                                        'path': cruft,
                                        'makos': makos
                                }, 'linking');
                                linkAndMoveFile({
                                        'client': MANTA_CLIENT,
                                        'path': cruft,
                                        'makos': makos
                                }, subcb);
                        }
                ]
        }, cb);
}


//--- Pipeline

function createMorayClients(_, cb) {
        function createClient(shard, subcb) {
                var client = moray.createClient({
                        log: LOG,
                        connectTimeout: MORAY_CONNECT_TIMEOUT,
                        host: shard,
                        port: MORAY_PORT
                });

                client.on('error', function (err) {
                        LOG.error(err, 'moray client error');
                });

                client.on('connect', function () {
                        LOG.info({ shard: shard }, 'Connected to shard.');
                        return (subcb(null, client));
                });
        }

        vasync.forEachParallel({
                'inputs': _.shards,
                'func': createClient
        }, function (err, res) {
                if (err) {
                        return (cb(err));
                }
                if (res.successes.length !== _.shards.length) {
                        return (cb(new Error('number of clients didn\'t ' +
                                             'match number of shards')));
                }
                _.morays = res.successes;
                return (cb());
        });
}


function findDoObjects(_, cb) {
        getObjectsInDir(MANTA_DO_DIR, function (err, objs) {
                if (err) {
                        return (cb(err));
                }
                _.cruft = objs;
                LOG.info({
                        'cruft': _.cruft
                }, 'found cruft objects');
                return (cb());
        });
}


function createTmpDir(_, cb) {
        fs.mkdir(TMP_DIR, function (err) {
                if (err && err.code !== 'EEXIST') {
                        return (cb(err));
                }
                return (cb());
        });
}


function verifyAndLinkFiles(_, cb) {
        vasync.forEachPipeline({
                'func': verifyAndLinkFile,
                'inputs': _.cruft.map(function (c) {
                        //This is a little wonky but I need the _ to propagate
                        return ({
                                'cruft': c,
                                'opts': _
                        });
                })
        }, cb);
}


function closeMorayClients(_, cb) {
        _.morays.forEach(function (c) {
                c.close();
        });
        return (cb());
}



///--- Main

var _opts = parseOptions();
LOG.info(_opts, 'using opts');

vasync.pipeline({
        'arg': _opts,
        'funcs': [
                createMorayClients,
                findDoObjects,
                createTmpDir,
                verifyAndLinkFiles,
                closeMorayClients
        ]
}, function (err) {
        if (err) {
                LOG.fatal(err, 'Error.');
        } else {
                AUDIT.cronFailed = 0;
        }

        //Write out audit record.
        AUDIT.endTime = new Date();
        AUDIT.cronRunMillis = (AUDIT.endTime.getTime() -
                               AUDIT.startTime.getTime());
        LOG.info(AUDIT, 'audit');
        process.exit(AUDIT.cronFailed);
});
