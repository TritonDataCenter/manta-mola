/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * TableDemuxStream
 * Creates new streams for each table.
 *
 * Listen for the 'stream' event.
 */

var stream = require('stream');
var util = require('util');
var PassThrough = stream.PassThrough;
var Writable = stream.Writable;

module.exports = TableDemuxStream;

function TableDemuxStream() {
        var self = this;
        Writable.call(self, {
                objectMode: true // read objects
        });
        self.stream = null;
        self.table = null;
}
util.inherits(TableDemuxStream, Writable);


TableDemuxStream.prototype._write = function (entry, encoding, cb) {
        var self = this;
        var ok = true;
        if (entry.line === null) {
                self.table = null;
                self.stream.end();
        } else if (!self.table) {
                self.table = entry.table;
                self.stream = new PassThrough();
                self.emit('stream', self.stream, self.table);
                ok = self.stream.write(entry.line);
        } else {
                ok = self.stream.write(entry.line);
        }

        if (!ok) {
                self.stream.once('drain', cb);
        } else {
                setImmediate(cb);
        }
};
