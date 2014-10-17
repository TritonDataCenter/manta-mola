/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * SqlToJsonStream
 * Reads SQL and transforms it into newline-delmited JSON.
 *
 * Each table is preceded by a header row containing the table's name and
 * columns:
 * COPY table_name (cola, colb, mtime) FROM stdin;
 * to
 * {"name":"table_name","keys":["cola","colb","mtime"]}
 *
 * Each row's values are put into an array and emitted as an "entry" object:
 * value1     value2    2013-06-19 03:25:44.399571
 * to
 * {"entry":["value1","value2","2013-06-19 03:25:44.399571"]}
 */

var assert = require('assert-plus');
var util = require('util');
var Transform = require('stream').Transform;

/* JSSTYLED */
var PG_REGEX_TABLE = /^COPY ([\w]+) \(((?:[\"?\w\"?]+[\,]?[ ]?){1,})\) FROM stdin\;$/;
var PG_TABLE_END = '\\.';
var DOUBLE_BACKSLASH_REGEX = /\\\\/g;

module.exports = SqlToJsonStream;

// -- Helpers

// strip out "" in certain keys that conflict with pg keywords
function stripQuotes(keys) {
        return keys.map(function (k) {
                if (k[0] === '"' && k[k.length - 1] === '"') {
                        return (k.substr(1, k.length - 2));
                } else {
                        return (k);
                }
        });
}



// -- API

/**
 * events:
 *    on('complete'): emitted if opts.tables is set and all specified tables
 *      have been found
 *    on('table', table): emits the JS object version of each table header row
 *    on('entry', entry): emits the JS object version of each table entry
 *
 * options:
 * opts.tables: (optional) array of strings. extract only the named tables. if
 *            empty or not specified, all tables are extracted.
 */
function SqlToJsonStream(opts) {
        var self = this;
        opts = opts || {};
        assert.object(opts, 'opts');
        assert.optionalArrayOfString(opts.tables, 'tables');
        opts.decodeStrings = false;
        Transform.call(self, opts);

        //this._writableState.objectMode = true; // read objects
        this._readableState.objectMode = true; // emit objects
        self.buffer = '';
        self.table = null;
        self.tables = opts.tables || [];
        self.numFound = 0;
        self.done = false;
}
util.inherits(SqlToJsonStream, Transform);


SqlToJsonStream.prototype._transform = function (line, encoding, cb) {
        var self = this;
        if (self.done) {
                cb();
                return;
        }
        if (!self.table) {
                self._startTable(line);
        } else if (line === PG_TABLE_END) {
                self._endTable();
        } else {
                self._row(line);
        }
        cb();
};


SqlToJsonStream.prototype._startTable = function (line) {
        var self = this;

        var schema = PG_REGEX_TABLE.exec(line);
        if (!schema) {
                return;
        }

        var name = schema[1];
        if (self.tables.length && self.tables.indexOf(name) < 0) {
                return;
        }

        self.table = name;

        var keys = stripQuotes(schema[2].split(', '));

        var row = {
                name: name,
                keys: keys
        };
        var entry = {
                table: name,
                line: JSON.stringify(row) + '\n'
        };

        self.emit('table', name);
        self.emit('entry', entry);
        self.push(entry);
};


SqlToJsonStream.prototype._row = function (line) {
        var self = this;

        // MANATEE-120 Replace double backslashes with single backslashes
        // since pg_dump adds its own backslashes.
        var row = {
                entry: line.replace(DOUBLE_BACKSLASH_REGEX, '\\').split('\t')
        };
        var entry = {
                table: self.table,
                line: JSON.stringify(row) + '\n'
        };
        self.emit('entry', entry);
        self.push(entry);
};


SqlToJsonStream.prototype._endTable = function () {
        var self = this;
        if (self.table) {
                ++self.numFound;
                self.push({
                        table: self.table,
                        line: null
                });
                if (self.numFound === self.tables.length) {
                        self.push(null);
                        self.done = true;
                        self.emit('complete');
                }
                self.table = null;
        }
};
