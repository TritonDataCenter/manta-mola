// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');
var events = require('events');
var carrier = require('carrier');



///--- Globals
var GRACE_PERIOD_MILLIS = 1000 * 60 * 60 * 24 * 2;  //2 days



///--- API

/**
 * Reads the sorted rows from a transformed PG dump and emits actions that
 * should be taken to clean up mako and moray.
 *
 * 'mako' objects have the following fields:
 *   - url: The URL to the mako node
 *   - serverUuid: The uuid of the mako server
 *   - zoneUuid: The zone uuid
 *   - owner: The owner
 *   - objectId: The object id
 *   - toString(): Outputs the row so that it can be sorted against other
 *                 mako rows.
 *
 * 'moray' objects have the following fields:
 *   - morayHostname: The hostname of the moray shard
 *   - objectId: The objectId
 *   - date: The date for the record.
 * The objectId + the date is the primary key for figuring out what moray
 * record to purge.
 */
function GarbageCollector(reader, listener) {
        var self = this;
        var prev;
        self.carrier = carrier.carry(reader);

        if (listener) {
                self.addListener('moray', listener);
                self.addListener('mako', listener);
        }

        self.carrier.on('line', function (line) {
                var curr = transformToObject(line);
                curr.line = line;
                takeAction(self, prev, curr);
                prev = curr;
                curr = null;
        });

        self.carrier.on('end', function () {
                //We have to act on the last line
                takeAction(self, prev);
                self.emit('end');
        });
}

util.inherits(GarbageCollector, events.EventEmitter);
module.exports = GarbageCollector;



///--- Helpers

function transformToObject(line) {
        var parts = line.split('\t');
        return {
                objectId: parts[0],
                date: new Date(parts[1]),
                type: parts[2],
                obj: parts[3],
                morayHostname: parts[4]
        };
}


//Given 2 sorted rows, the 2nd row contents determines what happens to the 1st
// row.  So here we take action on the previous row.
function takeAction(gc, prev, curr) {
        //Skip all live objects.
        if (prev && prev.type === 'dead') {
                //If we find a future record of the object, we only need to
                // clean up the delete record.
                if (curr && prev.objectId === curr.objectId) {
                        emitMorayActions(gc, prev);
                } else {
                        var now = new Date();
                        if ((now - prev.date) > GRACE_PERIOD_MILLIS) {
                                emitMakoActions(gc, prev);
                                emitMorayActions(gc, prev);
                        }
                }
        }
}


function emitMorayActions(gc, obj) {
        gc.emit('moray', {
                morayHostname: obj.morayHostname,
                objectId: obj.objectId,
                date: obj.date,
                toString: function () {
                        return (this.morayHostname + '\t' +
                                this.objectId + '\t' +
                                this.date.toISOString());
                }
        });
}


function emitMakoActions(gc, obj) {
        var tableInfo = JSON.parse(obj.obj);
        var objInfo = tableInfo['_value'];
        if (!objInfo) {
                gc.emit('error', obj.line);
                return;
        }
        var owner = objInfo.owner;
        var objectId = objInfo.objectId;
        var sharks = objInfo.sharks;
        if (!owner || !objectId || !sharks) {
                gc.emit('error', obj.line);
                return;
        }
        for (var i = 0; i < sharks.length; ++i) {
                var shark = sharks[i];
                gc.emit('mako', {
                        url: shark.url,
                        serverUuid: shark.server_uuid,
                        zoneUuid: shark.zone_uuid,
                        owner: owner,
                        objectId: objectId,
                        toString: function () {
                                return (this.url + '\t' +
                                        this.serverUuid + '\t' +
                                        this.zoneUuid + '\t' +
                                        this.owner + '\t' +
                                        this.objectId);
                        }
                });
        }
}
