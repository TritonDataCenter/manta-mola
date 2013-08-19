// Copyright 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var vasync = require('vasync');

function startsWith(str, prefix) {
        return (str.slice(0, prefix.length) === prefix);
}

function endsWith(str, suffix) {
        return (str.indexOf(suffix, str.length - suffix.length) !== -1);
}

function findLatestMorayDumps(opts, cb) {
        assert.object(opts, 'opts');
        assert.string(opts.mantaUser, 'opts.mantaUser');
        assert.string(opts.shard, 'opts.shard');
        assert.object(opts.mantaClient, 'opts.mantaClient');
        assert.optionalString(opts.dir, 'opts.dir');

        var dir = opts.dir ||
                '/' + opts.mantaUser + '/stor/manatee_backups/' + opts.shard;
        opts.mantaClient.ls(dir, {}, function (err, res) {
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
                        //Assume that if there's no further directories to walk
                        // down, we're done.
                        if (dirs.length === 0) {
                                cb(null, {
                                        directory: dir,
                                        objects: objs
                                });
                                return;
                        }
                        dirs.sort(function (a, b) { return (b - a); });
                        dir += '/' + dirs[0];
                        findLatestMorayDumps({
                                mantaUser: opts.mantaUser,
                                shard: opts.shard,
                                mantaClient: opts.mantaClient,
                                dir: dir
                        }, cb);
                });
        });
}

function findTablesForShards(opts, cb) {
        assert.object(opts, 'opts');
        assert.string(opts.mantaUser, 'opts.mantaUser');
        assert.arrayOfString(opts.shards, 'opts.shards');
        assert.object(opts.mantaClient, 'opts.mantaClient');
        assert.arrayOfString(opts.tablePrefixes, 'opts.tablePrefixes');

        var shards = opts.shards;
        if (shards.length === 0) {
                cb(new Error('No shards specified.'));
                return;
        }

        vasync.forEachParallel({
                func: findLatestMorayDumps,
                inputs: shards.map(function (s) {
                        return ({
                                mantaUser: opts.mantaUser,
                                mantaClient: opts.mantaClient,
                                shard: s
                        });
                })
        }, function (err, results) {
                if (err) {
                        cb(err);
                        return;
                }
                if (results.successes.length !== shards.length) {
                        cb(new Error('Couldnt find latest backup for all ' +
                                     'shards.'));
                        return;
                }

                var objects = [];

                for (var i = 0; i < shards.length; ++i) {
                        var res = results.successes[i];
                        var dir = res.directory;
                        var objs = res.objects;
                        var tp = opts.tablePrefixes.map(function (p) {
                                return (p);
                        });

                        //Search the objects for the tables we need to process
                        for (var j = 0; j < objs.length; ++j) {
                                var obj = objs[j];
                                for (var k = 0; k < tp.length; ++k) {
                                        if (startsWith(obj, tp[k])) {
                                                objects.push(dir + '/' + obj);
                                        }
                                }
                        }
                }

                cb(null, objects);
        });
}

module.exports = {
        endsWith: endsWith,
        findLatestMorayDumps: findLatestMorayDumps,
        findTablesForShards: findTablesForShards,
        startsWith: startsWith
};
