// Copyright 2012 Joyent, Inc.  All rights reserved.

var carrier = require('carrier');
var assert = require('assert-plus');
var events = require('events');
var SchemaReader = require('./schema_reader');
var path = require('path');
var util = require('util');



///--- API

/**
 * This tranforms rows useable for auditing.
 */
function AuditRowTransformer(opts, listener) {
        var self = this;
        var reader = opts.reader;
        var mantaKey = opts.mantaKey;

        if (listener) {
                self.addListener('row', listener);
        }

        // Example: /poseidon/stor/mako/2.stor.coal.joyent.us
        var kparts = mantaKey.split('/');
        if (kparts[3] === 'mako') {
                var storageId = kparts[4];
                var c = carrier.carry(reader);

                c.on('line', function (line) {
                        var parts = line.split('\t');
                        // Doing a "basename" means we're discarding the
                        // owner for objects under /manta/[owner]/[objectid]
                        // and the fact that it is a GCed object for objects
                        // under /manta/tombstone/[date]/[objectid]
                        var objectId = path.basename(parts[0]);
                        self.emit('row', {
                                'objectId': objectId,
                                'storageId': storageId,
                                'type': 'mako',
                                toString: function () {
                                        return (this.objectId + '\t' +
                                                this.storageId + '\t' +
                                                this.type);
                                }
                        });
                });

        // Example: /poseidon/stor/manatee_backups/1.moray.coal.joyent.us/\
        //          2013/05/09/16/manta-2013-05-09-16-16-24.gz
        } else {
                //Moray Transforming
                var shard = kparts[4];
                var schemaReader = new SchemaReader(reader);

                schemaReader.on('object', function (o) {
                        var v = o['_value'];
                        if (v['type'] !== 'object') {
                                return;
                        }
                        for (var i = 0; i < v['sharks'].length; ++i) {
                                var shark = v['sharks'][i];
                                self.emit('row', {
                                        'objectId': v['objectId'],
                                        'storageId': shark['manta_storage_id'],
                                        'type': 'moray',
                                        'key': o['_key'],
                                        'shard': shard,
                                        toString: function () {
                                                return (this.objectId + '\t' +
                                                        this.storageId + '\t' +
                                                        this.type + '\t' +
                                                        this.key + '\t' +
                                                        this.shard);
                                        }
                                });
                        }
                });

                schemaReader.on('error', function (err) {
                        self.emit('error', err);
                });
        }
}

util.inherits(AuditRowTransformer, events.EventEmitter);
module.exports = AuditRowTransformer;
