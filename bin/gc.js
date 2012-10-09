#!/usr/bin/env node
// -*- mode: js -*-
// Copyright 2012 Joyent, Inc.  All rights reserved.

var lib = require('../lib');



///--- Main

var _garbageCollector = lib.createGarbageCollector(process.stdin);
_garbageCollector.on('moray', function (moray) {
        console.log('moray\t' + moray.toString());
});

_garbageCollector.on('mako', function (mako) {
        console.log('mako\t' + mako.toString());
});

_garbageCollector.on('error', function(line) {
        console.error('error\t' + line);
});

process.stdin.resume();
