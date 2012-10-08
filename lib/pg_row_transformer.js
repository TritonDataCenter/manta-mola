// Copyright 2012 Joyent, Inc.  All rights reserved.

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
 *  - moray_hostname: The moray host from which the record originated.
 *
 * It also exposes a toString method for getting the row in a format
 * suitable for sorting.
 */
function PgRowTransformer(opts, listener) {
        var self = this;
        var reader = opts.reader;
        var dump_date = opts.dump_date;
        var least_dump_date = opts.least_dump_date;
        var moray_hostname = opts.moray_hostname;

        self.schema_reader = new SchemaReader(reader);

        if (listener) {
                self.addListener('row', listener);
        }

        self.schema_reader.on('object', function (obj) {
                var table = obj['__table'];
                var row;
                if (table === PG_LIVE_MANTA_TABLE_NAME) {
                        row = transformLive(obj, dump_date, moray_hostname);
                } else if (table === PG_DEAD_MANTA_TABLE_NAME) {
                        row = transformDead(obj, least_dump_date,
                                            moray_hostname);
                }
                //If we get rows from other tables, just ignore them.
                if (row) {
                        self.emit('row', row);
                }
        });

        self.schema_reader.on('end', function () {
                self.emit('end');
        });
}

util.inherits(PgRowTransformer, events.EventEmitter);
module.exports = PgRowTransformer;



///--- Helpers

/**
 * Transforms a "live" row to:
 * [objectId] + [TAB] + [ISO8601Date] + [TAB] + 'live'
 */
function transformLive(obj, dump_date, moray_hostname) {
        assert.string(obj['__table'], PG_LIVE_MANTA_TABLE_NAME);
        var value = obj['_value'];
        if (value.type !== 'object') {
                return (null);
        }
        return ({
                objectId: value.objectId,
                date: dump_date,
                type: 'live',
                obj: obj,
                toString: function () {
                        return (this.objectId + '\t' + this.date.toISOString() +
                                '\t' + this.type);
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
function transformDead(obj, least_dump_date, moray_hostname) {
        assert.string(obj['__table'], PG_DEAD_MANTA_TABLE_NAME);
        var value = obj['_value'];
        var date = new Date(value.mtime);
        //Discard any dump records that came after the lowest dump time
        // of all the dumps.
        if (date > least_dump_date) {
                return (null);
        }
        return ({
                objectId: value.objectId,
                date: date,
                type: 'dead',
                obj: obj,
                moray_hostname: moray_hostname,
                toString: function () {
                        return (this.objectId + '\t' + this.date.toISOString() +
                                '\t' + this.type + '\t' +
                                JSON.stringify(this.obj) + '\t' +
                                this.moray_hostname);
                }
        });
}
