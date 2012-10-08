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
 */
function GarbageCollector(reader, listener) {
        var self = this;
        var prev;
        self.carrier = carrier.carry(reader);

        if (listener) {
                self.addListener('actions', listener);
        }

        self.carrier.on('line', function (line) {
                var curr = transformToObject(line);
                var actions = takeAction(prev, curr);
                if (actions) {
                        self.emit('actions', actions);
                }
                prev = curr;
                curr = null;
        });

        self.carrier.on('end', function () {
                //We have to act on the last line
                var actions = takeAction(prev);
                if (actions) {
                        self.emit('actions', actions);
                }
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
                moray_hostname: parts[4]
        };
}

//Given 2 sorted rows, the 2nd row contents determines what happens to the 1st
// row.  So here we take action on the previous row.
function takeAction(prev, curr) {
        //Skip all live objects.
        if (prev && prev.type === 'dead') {
                //If we find a future record of the object, we only need to
                // clean up the delete record.
                if (curr && prev.objectId === curr.objectId) {
                        return (getMorayActions(prev));
                } else {
                        var now = new Date();
                        if ((now - prev.date) > GRACE_PERIOD_MILLIS) {
                                var actions = getMakoActions(prev);
                                actions.concat(getMorayActions(prev));
                                return (actions);
                        }
                }
        }
        return (null);
}

function getMorayActions(obj) {
        return ([ {
                type: 'moray',
                moray_hostname: obj.moray_hostname,
                objectId: obj.objectId,
                date: obj.date,
                toString: function () {
                        return (this.type + '\t' +
                                this.moray_hostname + '\t' +
                                this.objectId + '\t' +
                                this.date.toISOString());
                }
        }]);
}

function getMakoActions(obj) {
        var actions = [];
        var obj_info = JSON.parse(obj.obj);
        var owner = obj.owner;
        var objectId = obj.objectId;
        var sharks = obj_info.sharks;
        for (var i = 0; i < sharks.length; ++i) {
                var shark = sharks[i];
                actions.push({
                        type: 'mako',
                        url: shark.url,
                        server_uuid: shark.server_uuid,
                        zone_uuid: shark.zone_uuid,
                        owner: owner,
                        objectId: objectId,
                        toString: function () {
                                return (this.type + '\t' +
                                        this.url + '\t' +
                                        this.server_uuid + '\t' +
                                        this.zone_uuid + '\t' +
                                        this.owner + '\t' +
                                        this.objectId);
                        }
                });
        }
        return (actions);
}
