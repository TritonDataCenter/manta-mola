// Copyright 2012 Joyent, Inc.  All rights reserved.

var util = require('util');
var events = require('events');
var carrier = require('carrier');



///--- API

/**
 * The auditor takes the output from the audit_pg_transform after it has
 * been mapped and sorted.  The rows should be sorted like so:
 *
 *   o1   storage1   mako
 *   o1   storage2   mako
 *   o1   storage1   moray ...
 *   o1   storage2   moray ...
 *   o1   storage1   moray ...  //Can occur more than once due to links
 *   o1   storage2   moray ...  //Can occur more than once due to links
 *   o2   ...
 *
 *  The crux of the algorithm is that we can build the set of storage nodes
 *  that an object is on from the first N mako lines, then verify that
 *  all moray records have the correct set.
 */
function Auditor(opts, listener) {
        var self = this;
        var reader = opts.reader;

        var currMako = {
                objectId: '',
                storageNodes: []
        };

        if (listener) {
                self.addListener('problem', listener);
        }

        self.carrier = carrier.carry(reader);

        self.carrier.on('line', function (line) {
                var parts = line.split('\t');
                var objectId = parts[0];
                var storageId = parts[1];
                var type = parts[2];
                if (type === 'mako') {
                        if (currMako.objectId === objectId) {
                                currMako.storageNodes.push(storageId);
                        } else {
                                currMako.objectId = objectId;
                                currMako.storageNodes = [storageId];
                        }
                } else {
                        if ((currMako.objectId !== objectId) ||
                            (currMako.storageNodes.indexOf(storageId) === -1)) {
                                self.emit('problem', line);
                        }
                }
        });

        self.carrier.on('end', function () {
                self.emit('end');
        });
}

util.inherits(Auditor, events.EventEmitter);
module.exports = Auditor;
