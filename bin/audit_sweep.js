#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var bunyan = require('bunyan');
var fs = require('fs');
var getopt = require('posix-getopt');
var lib = require('../lib');
var path = require('path');



///--- Global Objects

var NAME = 'mola-audit-sweep';
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: NAME,
        stream: process.stdout
});



///--- Helpers

function parseOptions() {
        var option;
        var opts = { log: LOG };
        var parser = new getopt.BasicParser('e:f:s:', process.argv);

        while ((option = parser.getopt()) !== undefined) {
                if (option.error) {
                        usage();
                }

                switch (option.option) {
                case 'e':
                        opts.errorFile = option.optarg;
                        break;
                case 'f':
                        opts.file = option.optarg;
                        break;
                case 's':
                        opts.storageListFile = option.optarg;
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        if (!opts.errorFile) {
                usage('-e [file] is a required argument');
        }
        if (!opts.file) {
                usage('-f [file] is a required argument');
        }
        if (!opts.storageListFile) {
                usage('-s [storage_list_file] is a required argument');
        }

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str = 'usage: ' + path.basename(process.argv[1]);
        str += [
                ' [-f file] [-e error file] [-s storage_list_file]',
                '',
                'The file is the output from an audit job.  Simply mget the',
                'file and point the app at it.',
                '',
                'The error file will contain all lines that weren\'t able to',
                'be cleaned up for one reason or another.  Check the log for',
                'the reason behind the failure.  It is safe to run the sweeper',
                'using the error file as input (-f [error_file]).',
                '',
                'The storage_list_file is a list of all the storage nodes and',
                'the corresponding datacenters that exist in manta.  This app',
                'uses the brute-force mechanism of lookat at each one for',
                'every object that is in an error state.  The format of the',
                'file is:',
                '   manta_storage_id datacenter',
                '   ...',
                '',
                'And can be generated with the following one-liner from the',
                'headnode in a region:',
                '   sdc-sapi /instances?service_uuid=$(sdc-sapi \\',
                '      /services?name=storage | json -Ha uuid) | \\',
                '      json -Ha metadata.MANTA_STORAGE_ID \\',
                '               metadata.DATACENTER',
                '',
                'Be very careful that the above is the complete list.'
        ].join('\n');
        console.error(str);
        process.exit(1);
}



///--- Main

var _opts = parseOptions();
_opts.reader = fs.createReadStream(_opts.file);
_opts.storageList = [];

// Create the storage list
var _data = fs.readFileSync(_opts.storageListFile, 'utf8');
var _lines = _data.split('\n');
for (var _i = 0; _i < _lines.length; ++_i) {
        var _line = _lines[_i];
        if (!_line) {
                continue;
        }
        var _parts = _line.split(' ');
        if (_parts.length !== 2) {
                LOG.error({ line: _line }, 'invalid line');
                process.exit(1);
        }
        _opts.storageList.push({
                'manta_storage_id': _parts[0],
                'datacenter': _parts[1]
        });

}
LOG.info({ storageList: _opts.storageList }, 'storage list');

// Sweep!
var _auditSweeper = lib.createAuditSweeper(_opts);

_auditSweeper.run(_opts, function (err, res) {
        if (err) {
                console.error(err);
                process.exit(1);
        }
        if (res && res.errorLines && res.errorLines.length > 0) {
                fs.writeFileSync(_opts.errorFile,
                                 res.errorLines.join('\n'));
        }
        LOG.info({
                audit: true,
                stats: _auditSweeper.getStats()
        }, 'audit');
        _auditSweeper.close(function () {
                LOG.info('Done!');
        });
});
