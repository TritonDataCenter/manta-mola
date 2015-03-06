/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var events = require('events');
var carrier = require('carrier');
var common = require('./common');
var fs = require('fs');
var util = require('util');
var vasync = require('vasync');



///--- API

/**
 * See bin/rebalance.js for the context in which this interface is used.  In
 * short, the rebalancer reads through a list of objects from the "manta" bucket
 * in moray, figures out which ones need to be rebalanced, chooses a new shark,
 * and outputs a list of structures that will be consumed by processes on the
 * mako nodes in order to finally rebalance the objects.
 *
 * By default, if the complete set of sharks is spread across at least two
 * datacenters, then we rebalance any objects where there are more than two
 * copies but all copies are in the same datacenter.  If mantaStorageId is
 * specified, then we also move all copies from that shark.
 *
 * Required arguments include:
 *
 *     reader                   Input stream
 *     (required)
 *
 *     sharks                   Set of available sharks, in an object keyed
 *     (required)               by datacenter.  See bin/rebalance.js for
 *                              details.
 *
 *     dir                      Path to local temporary directory
 *     (required)
 *
 *
 * Optional arguments include:
 *
 *     mantaStorageId           If specified, then all copies of all objects on
 *     (optional)               this shark will be migrated to other sharks.
 */
function Rebalancer(opts) {
        var self = this;
        var mantaStorageId = opts.mantaStorageId;
        var reader = opts.reader;
        var sharks = opts.sharks;
        var singleDc = Object.keys(sharks).length == 1;
        var dir = opts.dir;
        var barrier = vasync.barrier();

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

                /*
                 * These should already have been filtered out, but check again
                 * here just in case.
                 */
                if (o.type !== 'object') {
                        return;
                }

                /*
                 * If this object id was seen for the last row, just choose what
                 * we chose last time.  In general, our input is sorted by
                 * objectid already, and we may see the same objectid more than
                 * once if it has more than one link in the metadata tier.  We
                 * still want to emit more than one record because we'll need to
                 * update the corresponding Moray record for each link.
                 *
                 * We only fix one problem at a time.  If we've already
                 * identified a problem (e.g., the object is not spread across
                 * enough DCs), then we'll produce a record to fix that without
                 * checking for other problems (e.g., the object is on a shark
                 * that we're trying to avoid using).
                 */
                var repl;
                if (self.lastObjectId === o.objectid) {
                        repl = self.lastRepl;
                }

                if (repl === undefined && !singleDc) {
                        repl = checkUniqueDcs(o, sharks);
                }

                if (repl === undefined && mantaStorageId !== undefined) {
                        repl = checkForMantaStorageId(o, sharks,
                                                      mantaStorageId);
                }

                /* Hooray!  There's nothing to be done for this object. */
                if (repl === undefined) {
                        return;
                }

                /*
                 * Keep track of the last objectid we've seen and the last
                 * rebalance record for it so that we can emit the same one for
                 * subsequent copies of the same object.  Since our input is
                 * expected to be sorted by objectid, this is sufficient to
                 * ensure all rebalance records for this object contain the
                 * same sharks.
                 */
                self.lastObjectId = o.objectid;
                self.lastRepl = repl;

                /*
                 * Write the record describing the migration to a file
                 * associated with the new shark.
                 */
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

        /*
         * When we've finished reading all of the input, close all of the output
         * streams that we opened.
         */
        barrier.start('input');
        self.carrier.on('end', function () {
                var streamKeys = Object.keys(fileStreams);
                streamKeys.forEach(function (filename) {
                        var stream = fileStreams[filename];
                        barrier.start('close ' + filename);
                        stream.end('', null, function () {
                                barrier.done('close ' + filename);
                        });
                        stream.end();
                });

                barrier.on('drain', function () { self.emit('end'); });
                barrier.done('input');
        });
}

util.inherits(Rebalancer, events.EventEmitter);
module.exports = Rebalancer;



//--- Helpers

/*
 * Each of the check* helpers looks at a single object record ("o") and the set
 * of available sharks ("sharks") and checks whether the object violates one of
 * the policies we're enforcing here (e.g., "multiple copies of an object are
 * spread across at least two datacenters").  If so, the helper selects a new
 * shark from "sharks" that satisfies the policy and returns an object
 * containing "oldShark" and "newShark" properties.  If no shark satisfies the
 * policy, an error is thrown.  This is not expected, and the error should not
 * be handled.
 */

/*
 * Policy: multiple copies of an object should be spread across at least two
 * datacenters.
 */
function checkUniqueDcs(o, sharks) {
        var v = o._value;
        if (v.sharks.length < 2) {
                /* There's only one copy, so it doesn't matter where it is. */
                return (undefined);
        }

        var uniqueDcs = [];
        v.sharks.map(function (s) {
                if (uniqueDcs.indexOf(s.datacenter) === -1) {
                        uniqueDcs.push(s.datacenter);
                }
        });

        if (uniqueDcs.length > 1) {
                /*
                 * If the multiple copies are spread across at least two
                 * datacenters, then we're all set.
                 */
                return (undefined);
        }

        /*
         * Given that we're now looking at an object that's got multiple copies,
         * all in the same datacenter, we can replace any copy, and we
         * arbitrarily choose the first.
         */
        var oldShark = v.sharks[0];
        var newShark = chooseSharkDifferentDc(oldShark, sharks);
        return ({
                oldShark: oldShark,
                newShark: newShark
        });
}

/*
 * Given a shark "old" and a set of available sharks "sharks", select a shark
 * from "sharks" that's in a different datacenter from "old".
 */
function chooseSharkDifferentDc(old, sharks) {
        var dc = old.datacenter;
        var pdcs = Object.keys(sharks);
        pdcs.splice(pdcs.indexOf(dc), 1);
        var ndc = pdcs[Math.floor(Math.random() * pdcs.length)];
        if (!ndc) {
                console.error({ old: old,
                                sharks: sharks },
                              'unable to choose new dc for object');
                throw new Error('unable to choose new dc for object');
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

/*
 * Policy: no objects should be stored on shark "mantaStorageId".  Recall that
 * we only fix one problem at a time, so we'll only move one copy from this
 * shark in this round.
 */
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

/*
 * Given a shark "old", an object "o", and a set of available sharks "sharks",
 * select a new shark for storing a new copy of "o" that will be removed from
 * "o".  We avoid using a shark in the same datacenter as any other copy.
 *
 * This doesn't handle some wacky edge cases, like if the replication factor is
 * high and there's no shark the object can go to in the same datacenter that
 * the old shark is in.  In practice, though, this should never happen.
 */
function chooseShark(old, o, sharks) {
        /*
         * Eliminate all the datacenters that the object is currently in except
         * for the datacenter whose copy is being replaced.
         */
        var pdcs = Object.keys(sharks);
        var mids = [];
        o._value.sharks.forEach(function (shark) {
                mids.push(shark.manta_storage_id);
                if (shark.manta_storage_id !== old.manta_storage_id) {
                        pdcs.splice(pdcs.indexOf(shark.datacenter), 1);
                }
        });

        /*
         * If the object is already in all datacenters due to high replication
         * factor, just find a shark in the same datacenter as the existing
         * copy.
         */
        if (pdcs.length === 0) {
                pdcs = [old.datacenter];
        }
        var ndc = pdcs[Math.floor(Math.random() * pdcs.length)];
        var shs = [];
        /* Only take the sharks that the object isn't already on. */
        sharks[ndc].forEach(function (shark) {
                if (mids.indexOf(shark.manta_storage_id) === -1) {
                        shs.push(shark);
                }
        });
        /* We don't want to continue the job if we can't choose a shark. */
        if (shs.length === 0) {
                console.error({ object: o },
                              'unable to choose shark for object');
                throw new Error('unable to choose shark for object');
        }
        return (shs[Math.floor(Math.random() * shs.length)]);
}
