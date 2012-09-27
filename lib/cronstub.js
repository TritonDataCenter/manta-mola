/*
 * Copyright (c) 2012, Joyent, Inc. All rights reserved.
 *
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
