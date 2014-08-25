/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * A silly stub to log a few times and exit.
 */
var bunyan = require('bunyan');
var log = bunyan.createLogger({
        name: 'cronstub.js'
});

var count = 0;

function logHello() {
        log.info('Hello (' + count + ')!');
        if (++count < 5) {
                setTimeout(logHello, 2000);
        }
}

logHello();
