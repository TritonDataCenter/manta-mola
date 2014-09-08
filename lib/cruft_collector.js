/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');
var events = require('events');
var carrier = require('carrier');



///--- API

/**
 * The auditor takes the output from the audit_pg_transform after it has
 * been mapped and reverse sorted.  The rows should be sorted like so:
 *
 *   o1   moray
 *   o1   moray  //Can occur more than once due to links
 *   o1   mako  storage2  owner  bytes  createTime
 *   o1   mako  storage1  owner  bytes  createTime
 *   o2   ...
 *
 * The crux of the algorithm is that we can keep track of the last object
 * id for the moray lines and emit the mako lines if the object id isn't the
 * same (indicating that the object doesn't exist in moray... it is cruft)
 *
 * The output is a gc-compatible row (slightly rearranged from what's above)
 * mako  storageId  owner  objectId  bytes  createTime
 */
function CruftCollector(opts, listener) {
        var self = this;
        var reader = opts.reader;

        var currMoray = null;

        if (listener) {
                self.addListener('mako', listener);
        }

        self.carrier = carrier.carry(reader);

        self.carrier.on('line', function (line) {
                var parts = line.split('\t');
                var objectId = parts[0];
                var type = parts[1];
                if (type === 'moray') {
                        currMoray = objectId;
                } else {
                        if (currMoray !== objectId) {
                                // Need to rearrange the line so that it is
                                // compatible with GC (see above)
                                var p = line.split('\t');
                                var nline =
                                        p[1] + '\t' + //mako
                                        p[2] + '\t' + //storageId
                                        p[3] + '\t' + //owner
                                        p[0] + '\t' + //objectId
                                        p[4] + '\t' + //bytes
                                        p[5];  //createTime
                                self.emit('mako', nline);
                        }
                }
        });

        self.carrier.on('end', function () {
                self.emit('end');
        });
}

util.inherits(CruftCollector, events.EventEmitter);
module.exports = CruftCollector;
