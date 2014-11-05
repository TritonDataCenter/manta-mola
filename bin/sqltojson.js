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
var LineStream = require('lstream');
var manta = require('manta');
var path = require('path');
var zlib = require('zlib');
var dashdash = require('dashdash');
var SqlToJsonStream = require('../lib/sqltojson.js');
var TableDemux = require('../lib/table_demux.js');


function getOptions() {
        var options = [
                {
                        names: [ 'all', 'a' ],
                        type: 'bool',
                        help: 'Extract all tables.'
                },
                {
                        names: ['local', 'l'],
                        type: 'string',
                        help: 'Local scratch directory to save table dumps'
                },
                {
                        names: ['remote', 'r'],
                        type: 'string',
                        help: 'Manta directory to upload table dumps'
                },
                {
                        names: ['file', 'f'],
                        type: 'string',
                        help: 'Read from file.'
                },
                {
                        names: ['suffix'],
                        type: 'string',
                        help: 'Suffix to append to file & object names. ' +
                                'Generated from MANTA_INPUT_OBJECT by default.'
                },
                {
                        names: ['uncompressed'],
                        type: 'bool',
                        help: 'Leave output tables uncompressed.'
                },
                {
                        names: ['noScratch', 'no-scratch'],
                        type: 'bool',
                        help: 'Skip saving tables to local disk and upload ' +
                                'straight from memory to Manta.'
                },
                {
                        names: ['noUpload', 'no-upload'],
                        type: 'bool',
                        help: 'Stop after saving tables to local disk and ' +
                                'don\'t upload them to Manta.'
                },
                {
                        names: ['help', 'h'],
                        type: 'bool',
                        help: 'Print this help and exit.'
                }
        ];

        function usage() {
                console.log(
                'usage: node sqltojson.js -a -l /local/dir -r /remote/dir\n' +
                '       node sqltojson.js -r /remote/dir --no-scratch' +
                                'manta manta_delete_log\n' +
                'options:\n' +
                help);
        }

        var parser = dashdash.createParser({options: options});
        var help = parser.help().trimRight();
        var opts;
        try {
                opts = parser.parse(process.argv);
        } catch (e) {
                console.error('error: %s', e.message);
                usage();
                process.exit(1);
        }

        if (opts.help) {
                usage();
                process.exit(0);
        }

        if (opts.all && opts._args.length || !opts.all && !opts._args.length) {
                console.error('error: specify -a/--all or a list of tables');
                usage();
                process.exit(1);
        }

        if (opts.noScratch && opts.noUpload) {
                console.error('error: --no-upload and --no-scratch can not ' +
                        'be used together');
                usage();
                process.exit(1);
        }

        if (!opts.noScratch && !opts.local) {
                console.error('error: local scratch directory required');
                usage();
                process.exit(1);
        }

        if (!opts.noUpload && !opts.remote) {
                console.error('error: remote Manta directory required');
                usage();
                process.exit(1);
        }

        if (opts.local) {
                opts.local = path.resolve(opts.local);
        }

        if (opts.remote) {
                opts.remote = path.normalize(opts.remote);
        }

        if (opts.file) {
                opts.file = path.resolve(opts.file);
        }

        opts.log = bunyan.createLogger({
                name: 'SqlToJson'
        });

        opts.manta = manta.createBinClient({
                log: opts.log
        });

        if (!opts.suffix) {
                opts.suffix = getInputDate();
                opts.suffix += opts.uncompressed ? '.json' : '.gz';
        }

        return (opts);
}


function getInputDate() {
        // object name like moray-2014-10-12-00-00-28.gz
        if (!process.env.MANTA_INPUT_OBJECT) {
                return ('');
        }

        var name = process.env.MANTA_INPUT_OBJECT;

        // remove the file extension
        var base = path.basename(name, path.extname(name));

        var parts = base.split('-');
        return ('-' + parts.slice(1).join('-'));
}


function save(opts, stream, table) {
        var chain, zstream, fstream, mstream;
        var localPath = opts.local + '/' + table + opts.suffix;
        var mPath = opts.remote + '/' + table + opts.suffix;

        if (!opts.uncompressed) {
                zstream = zlib.createGzip();
                chain = stream.pipe(zstream);
        }

        if (!opts.noScratch) {
                fstream = fs.createWriteStream(localPath);
                chain = chain ? chain.pipe(fstream) : stream.pipe(fstream);
        }

        if (!opts.noUpload) {
                if (fstream) {
                        fstream.once('finish', function onDisk() {
                                fs.stat(localPath, function (err, stats) {
                                        if (err) {
                                                throw (err);
                                        }
                                        var o = {
                                                copies: 2,
                                                size: stats.size
                                        };
                                        var s = fs.createReadStream(localPath);
                                        var p = mPath;
                                        s.pause();
                                        function done(err2) {
                                                if (err2) {
                                                        throw (err2);
                                                }
                                        }
                                        s.on('open', function () {
                                                opts.manta.put(p, s, o, done);
                                        });
                                });
                        });
                } else {
                        mstream = opts.manta.createWriteStream(mPath);
                        chain.pipe(mstream);
                }
        }
}


function main() {
        var opts = getOptions();

        var lstream = new LineStream({
                highWaterMark: 0
        });
        var sqlstream = new SqlToJsonStream({
                tables: opts._args || []
        });
        var tabledemux = new TableDemux();
        var source = opts.file ? fs.createReadStream(opts.file) : process.stdin;

        tabledemux.on('stream', function (stream, table) {
                save(opts, stream, table);
        });

        // stop streaming if we found all the tables we care about
        sqlstream.once('complete', function () {
                source.unpipe(lstream);
        });

        // handle EPIPE
        process.stdout.once('error', function () {
                source.unpipe(sqlstream);
        });

        if (opts.remote) {
                opts.manta.mkdirp(opts.remote, function (err) {
                        if (err) {
                                console.error('error mkdir: ' + err);
                                process.exit(1);
                        }
                        source.pipe(lstream).pipe(sqlstream).pipe(tabledemux);
                });
        } else {
                source.pipe(lstream).pipe(sqlstream).pipe(tabledemux);
        }
}

if (require.main === module) {
        main();
}
