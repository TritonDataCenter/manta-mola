/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var helper = require('./helper.js');
var lib = require('../lib');
var MemoryStream = require('memorystream');
var util = require('util');



///--- Globals

var test = helper.test;



///--- Helpers

function l() {
        var line = '';
        for (var i = 0; i < arguments.length; ++i) {
                if (line.length > 1) {
                        line += '\t';
                }
                line += arguments[i];
        }
        return (line + '\n');
}



///--- Tests

test('test: no objects', function (t) {
        var data = l('o1', 'moray') +
                l('o1', 'mako', 's1', 'owner', '5', '1400000000', '50');
        var stream = new MemoryStream(data);
        var cruftCollector = lib.createCruftCollector({ reader: stream });
        var makos = [];

        cruftCollector.on('mako', function (m) {
                makos.push(m);
        });

        cruftCollector.on('end', function () {
                assert.equal(0, makos.length);
                t.end();
        });

        process.nextTick(function () {
                stream.end();
        });
});


test('test: some objects', function (t) {
        var data =
                l('o5', 'moray') +
                l('o5', 'moray') +
                l('o5', 'moray') +
                l('o5', 'moray') +
                l('o5', 'mako', 's1', 'owner', '5', '1400000000', '50') +
                l('o5', 'mako', 's2', 'owner', '5', '1400000000', '50') +
                l('o4', 'mako', 's1', 'owner', '5', '1400000000', '50') + //!
                l('o4', 'mako', 's2', 'owner', '5', '1400000000', '50') + //!
                l('o3', 'moray') +
                l('o3', 'mako', 's1', 'owner', '5', '1400000000', '50') +
                l('o2', 'mako', 's2', 'owner', '5', '1400000000', '50') + //!
                l('o1', 'moray') +
                l('o1', 'mako', 's1', 'owner', '5', '1400000000', '50');
        var stream = new MemoryStream(data);
        var cruftCollector = lib.createCruftCollector({ reader: stream });
        var makos = [];

        cruftCollector.on('mako', function (m) {
                //Pulling out the storage node, owner and object id.
                makos.push(m.split('\t').splice(1, 3).join('\t'));
        });

        cruftCollector.on('end', function () {
                assert.deepEqual([
                        's1\towner\to4',
                        's2\towner\to4',
                        's2\towner\to2'
                ], makos);
                t.end();
        });

        process.nextTick(function () {
                stream.end();
        });
});
