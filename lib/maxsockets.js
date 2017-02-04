/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

function checkNumber(num) {
        if (typeof (num) !== 'number' || isNaN(num) || !isFinite(num)) {
                return (false);
        }

        return (true);
}

function maxSocketsFixNeeded() {
        if (!process || !process.versions || !process.versions.node)
                return (true);

        var components = process.versions.node.split('.').map(function (c) {
                var ret = parseInt(c, 10);

                return (checkNumber(ret) ? ret : null);
        });

        /*
         * If we cannot identify the version, or if this is a version of
         * Node prior to 0.12.0, we must set maxSockets.
         */
        if (components.length !== 3 ||
            components[0] === null || components[1] === null ||
            (components[0] <= 0 && components[1] < 12)) {
                return (true);
        }

        /*
         * Subsequent versions of Node shipped with a default maxSockets
         * value of Infinity.
         */
        return (false);
}

/*
 * The maximum number of concurrent HTTP(S) connections that Node will make has
 * historically been quite low.  Prior to 0.12.0, the default "maxSockets"
 * value was 5.
 *
 * If a program needs to make a large number of concurrent HTTP requests,
 * particularly to the same server, it needs to increase the limit.  This must
 * be done before the Manta client library is imported, due to the way the
 * restify client interacts with these essentially global values.
 */
module.exports = function setMaxSockets(maxSockets) {
        if (!checkNumber(maxSockets) || maxSockets < 1) {
                throw (new Error('maxSockets must be a positive integer, ' +
                    'not "' + maxSockets + '"'));
        }

        if (!maxSocketsFixNeeded())
                return;

        require('http').globalAgent.maxSockets = maxSockets;
        require('https').globalAgent.maxSockets = maxSockets;
};
