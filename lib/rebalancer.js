// Copyright 2013 Joyent, Inc.  All rights reserved.

var events = require('events');
var carrier = require('carrier');
var common = require('./common');
var fs = require('fs');
var util = require('util');



///--- API

/**
 * The rebalancer reads through a list of objects from the manta table in moray,
 * figures out which ones need to be rebalanced, chooses a new shark and outputs
 * a structure that a process on each mako node will consume to rebalance
 * objects.
 */
function Rebalancer(opts, listener) {
        var self = this;
        var mantaStorageId = opts.mantaStorageId;
        var reader = opts.reader;
        var sharks = opts.sharks;
        //TODO: We'll need to plumb this through.
        var ignoreSameDc = opts.ignoreSameDc;
        var dir = opts.dir;

        var fileStreams = {};

        if (!common.endsWith(dir, '/')) {
                dir += '/';
        }

        self.carrier = carrier.carry(reader);
        self.carrier.on('line', function (line) {
                try {
                        var o = JSON.parse(line);
                } catch (e) {
                        console.error({
                                err: e,
                                line: line
                        }, 'problem JSON parsing line');
                        return;
                }

                //These should already have been filtered out, but just in case.
                if (o.type !== 'object') {
                        return;
                }

                //If this object id was seen for the last row, just choose what
                // we chose last time...
                var repl;
                if (self.lastObjectId === o.objectid) {
                        repl = self.lastRepl;
                }

                //We only fix one problem at a time....
                if (repl === undefined && !ignoreSameDc) {
                        repl = checkUniqueDcs(o, sharks);
                }

                if (repl === undefined && mantaStorageId !== undefined) {
                        repl = checkForMantaStorageId(o, sharks,
                                                      mantaStorageId);
                }

                //Hooray!
                if (repl === undefined) {
                        return;
                }

                //Cache for next time...
                self.lastObjectId = o.objectid;
                self.lastRepl = repl;

                var v = o._value;
                var oldShark = repl.oldShark;
                var newShark = repl.newShark;

                if (!fileStreams[newShark.manta_storage_id]) {
                        var filename = dir + newShark.manta_storage_id;
                        var news = fs.createWriteStream(filename);
                        fileStreams[newShark.manta_storage_id] = news;
                }

                var stream = fileStreams[newShark.manta_storage_id];
                var no = {
                        key: o._key,
                        morayEtag: o._etag,
                        newShark: newShark,
                        oldShark: oldShark,
                        md5: v.contentMD5,
                        objectId: v.objectId,
                        owner: v.owner,
                        etag: v.etag
                };

                stream.write(JSON.stringify(no, null, 0) + '\n');
        });

        self.carrier.on('end', function () {
                var streamKeys = Object.keys(fileStreams);
                if (streamKeys.length === 0) {
                        self.emit('end');
                        return;
                }
                var ended = 0;
                for (var i = 0; i < streamKeys.length; ++i) {
                        var stream = fileStreams[streamKeys[i]];
                        stream.end('', null, function () {
                                ++ended;
                                if (ended === streamKeys.length) {
                                        self.emit('end');
                                }
                        });
                }
        });
}

util.inherits(Rebalancer, events.EventEmitter);
module.exports = Rebalancer;



//--- Helpers

function checkUniqueDcs(o, sharks) {
        var v = o._value;
        if (v.sharks.length < 2) {
                return (undefined);
        }

        var uniqueDcs = [];
        v.sharks.map(function (s) {
                if (uniqueDcs.indexOf(s.datacenter) === -1) {
                        uniqueDcs.push(s.datacenter);
                }
        });

        if (uniqueDcs.length > 1) {
                return (undefined);
        }

        /**
         * The above logic guarentees that the object is located in only
         * one dc.  So we can safely choose the first record to replace.
         */
        var oldShark = v.sharks[0];
        var newShark = chooseSharkDifferentDc(oldShark, sharks);
        return ({
                oldShark: oldShark,
                newShark: newShark
        });
}


function chooseSharkDifferentDc(old, sharks) {
        var dc = old.datacenter;
        var pdcs = Object.keys(sharks);
        pdcs.splice(pdcs.indexOf(dc), 1);
        var ndc = pdcs[Math.floor(Math.random() * pdcs.length)];
        if (!ndc) {
                console.error({ old: old,
                                sharks: sharks },
                              'unable to choose new dc for object');
                throw new Error('unable to choose shark for object');
        }
        var shs = sharks[ndc];
        if (shs.length === 0) {
                console.error({ old: old,
                                sharks: sharks },
                              'unable to choose shark for object');
                throw new Error('unable to choose shark for object');
        }
        return (shs[Math.floor(Math.random() * shs.length)]);
}


function checkForMantaStorageId(o, sharks, mantaStorageId) {
        var v = o._value;
        for (var i = 0; i < v.sharks.length; ++i) {
                if (mantaStorageId === v.sharks[i].manta_storage_id) {
                        var oldShark = v.sharks[i];
                        var newShark = chooseShark(oldShark, o, sharks);
                        return ({
                                oldShark: oldShark,
                                newShark: newShark
                        });
                }
        }
}


//This doesn't handle some wacky edge cases, like if the replication factor is
// high and there's no shark the object can go to in the same dc that the
// old shark is on.  In practice, though, this should never happen.
function chooseShark(old, o, sharks) {
        //Clean out all the dcs that the object is currently in (minus the dc
        // that's being replaced)
        var pdcs = Object.keys(sharks);
        var mids = [];
        o._value.sharks.forEach(function (shark) {
                mids.push(shark.manta_storage_id);
                if (shark.manta_storage_id !== old.manta_storage_id) {
                        pdcs.splice(pdcs.indexOf(shark.datacenter), 1);
                }
        });
        //If the object is already in all dcs due to high replication factor,
        // just find one in the dc the old object is in...
        if (pdcs.length === 0) {
                pdcs = [old.datacenter];
        }
        var ndc = pdcs[Math.floor(Math.random() * pdcs.length)];
        var shs = [];
        //Only take the sharks that the object isn't already on...
        sharks[ndc].forEach(function (shark) {
                if (mids.indexOf(shark.manta_storage_id) === -1) {
                        shs.push(shark);
                }
        });
        //We don't want to continue the job if we can't choose a shark.
        if (shs.length === 0) {
                console.error({ object: o },
                              'unable to choose shark for object');
                throw new Error('unable to choose shark for object');
        }
        return (shs[Math.floor(Math.random() * shs.length)]);
}
