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
        var reader = opts.reader;
        var sharks = opts.sharks;
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

                if (o.type !== 'object') {
                        return;
                }

                var v = o._value;
                if (v.sharks.length < 2) {
                        return;
                }

                var uniqueDcs = [];
                v.sharks.map(function (s) {
                        if (uniqueDcs.indexOf(s.datacenter) === -1) {
                                uniqueDcs.push(s.datacenter);
                        }
                });

                if (uniqueDcs.length > 1) {
                        return;
                }

                /**
                 * The above logic guarentees that the object is located in only
                 * one dc.  So we can safely choose the first record to replace.
                 */
                var oldShark = v.sharks[0];
                var newShark = chooseShark(oldShark, sharks);

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

function chooseShark(old, sharks) {
        var dc = old.datacenter;
        var pdcs = Object.keys(sharks);
        pdcs.splice(pdcs.indexOf(dc), 1);
        var ndc = pdcs[Math.floor(Math.random() * pdcs.length)];
        var shs = sharks[ndc];
        return (shs[Math.floor(Math.random() * shs.length)]);
}
