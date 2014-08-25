/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var events = require('events');
var SchemaReader = require('./schema_reader');
var util = require('util');



///--- GLOBALS
var PG_LIVE_MANTA_TABLE_NAME = 'manta';
var PG_DEAD_MANTA_TABLE_NAME = 'manta_delete_log';



///--- API

/**
 * This tranforms pg-dumped rows to rows useable for garbage collection.
 *
 * The resulting rows emitted will have the following fields:
 *  - objectId: The object id
 *  - date: The time this row 'occured'
 *  - type: 'dead' or 'live'
 *  - object: The original object (only for the dead)
 *  - morayHostname: The moray host from which the record originated.
 *
 * It also exposes a toString method for getting the row in a format
 * suitable for sorting.
 */
function GcPgRowTransformer(opts, listener) {
        var self = this;
        var reader = opts.reader;
        var dumpDate = opts.dumpDate;
        var earliestDumpDate = opts.earliestDumpDate;
        var morayHostname = opts.morayHostname;

        self.schemaReader = new SchemaReader(reader);

        if (listener) {
                self.addListener('row', listener);
        }

        self.schemaReader.on('object', function (obj) {
                var table = obj['__table'];
                var row;
                if (table === PG_LIVE_MANTA_TABLE_NAME) {
                        row = transformLive(obj, dumpDate, morayHostname);
                } else if (table === PG_DEAD_MANTA_TABLE_NAME) {
                        row = transformDead(obj, earliestDumpDate,
                                            morayHostname);
                }
                //If we get rows from other tables, just ignore them.
                if (row) {
                        self.emit('row', row);
                }
        });

        self.schemaReader.on('end', function () {
                self.emit('end');
        });
}

util.inherits(GcPgRowTransformer, events.EventEmitter);
module.exports = GcPgRowTransformer;



///--- Helpers

/**
 * Transforms a "live" row to:
 * [objectId] + [TAB] + [ISO8601Date] + [TAB] + 'live'
 */
function transformLive(obj, dumpDate, morayHostname) {
        assert.string(obj['__table'], PG_LIVE_MANTA_TABLE_NAME);
        var value = obj['_value'];
        if (value.type !== 'object') {
                return (null);
        }
        return ({
                objectId: value.objectId,
                date: dumpDate,
                type: 'live',
                obj: obj,
                toString: function () {
                        return (this.objectId + '\t' +
                                this.date.toISOString() + '\t' +
                                this.type);
                }
        });
}


/**
 * Filters out rows that are newer than the earliest dump time, then transforms
 * a "dead" row to:
 *
 * [objectId] + [TAB] + [ISO8601Date] + [TAB] + 'dead' + [TAB] + \
 * [ORIGINAL OBJECT] + [TAB] + [Moray Hostname]
 */
function transformDead(obj, earliestDumpDate, morayHostname) {
        assert.string(obj['__table'], PG_DEAD_MANTA_TABLE_NAME);
        var value = obj['_value'];
        var date = new Date(parseInt(obj['_mtime'], 10));
        //Discard any dump records that came after the lowest dump time
        // of all the dumps.
        if (date > earliestDumpDate) {
                return (null);
        }
        return ({
                objectId: value.objectId,
                date: date,
                type: 'dead',
                obj: obj,
                morayHostname: morayHostname,
                toString: function () {
                        return (this.objectId + '\t' + this.date.toISOString() +
                                '\t' + this.type + '\t' +
                                JSON.stringify(this.obj) + '\t' +
                                this.morayHostname);
                }
        });
}
