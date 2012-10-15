// Copyright 2012 Joyent, Inc.  All rights reserved.

var carrier = require('carrier');
var crypto = require('crypto');
var events = require('events');


///--- Globals

var HASH_ALGO = 'md5';



///--- API

/**
 * This object will demux a stream into many other streams.
 */
function StreamDemux(opts, cb) {
        var self = this;
        self.carrier = carrier.carry(opts.input);
        self.streams = opts.output;
        self.keyFromLine = opts.keyFromLine || keyFromLine;
        self.selectStream = opts.selectStream || selectStream;

        self.carrier.on('line', function (line) {
                var key = self.keyFromLine(line);
                var stream = self.selectStream(key, self.streams);
                stream.write(line);
                stream.write('\n');
        });

        self.carrier.on('end', function () {
                for (var i = 0; i < self.streams.length; ++i) {
                        var stream = self.streams[i];
                        stream.destroySoon();
                }
                cb();
        });
}

module.exports = StreamDemux;



///--- Helpers

function keyFromLine(line) {
        return (line);
}


function selectStream(key, streams) {
        var hash = crypto.createHash(HASH_ALGO);
        hash.update(key);
        var digest = hash.digest('hex');
        var digestNumber = parseInt(digest.substr(0, 8), 16);
        var index = digestNumber % streams.length;
        return (streams[index]);
}
