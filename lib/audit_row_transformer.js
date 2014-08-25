/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

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
        //Example: 272d819c-e1b5-4bf6-8bf5-1bb8d946e5b9
        var uuidRegex = /^\w{8}\-\w{4}\-\w{4}-\w{4}-\w{12}$/;

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
                        //Explicitely filter out the tombstone data.  Because
                        // of the way muskie writes and GC works, the object
                        // should exist in the "live" portion of mako for as
                        // long as an object exists in the "live" portion of
                        // moray (and then the grace period)
                        if (parts[0].indexOf('/tombstone/') !== -1) {
                                return;
                        }
                        //Just to be sure, we test that the owner directory
                        // looks like a uuid, otherwise we could pick things up
                        // like nginx_tmp.
                        var pParts = parts[0].split('/');
                        if (!uuidRegex.test(pParts[2])) {
                                return;
                        }
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
