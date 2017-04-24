/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var MpuBatchStream = require('./mpuBatchStream');
var MpuVerifyStream = require('./mpuVerifyStream');
var mulrs = require('./mpuUnlinkLiveRecordStream');
var MpuUnlinkLiveRecordStream = mulrs.MpuUnlinkLiveRecordStream;
var MpuMorayCleanerStream = require('./mpuMorayCleanerStream');

function createMpuBatchStream(opts) {
        return (new MpuBatchStream(opts));
}

function createMpuVerifyStream(opts) {
        return (new MpuVerifyStream(opts));
}

function createMpuUnlinkLiveRecordStream(opts) {
        return (new MpuUnlinkLiveRecordStream(opts));
}

function createMpuMorayCleanerStream(opts) {
        return (new MpuMorayCleanerStream(opts));
}

module.exports = {
        createMpuBatchStream: createMpuBatchStream,
        createMpuVerifyStream: createMpuVerifyStream,
        createMpuUnlinkLiveRecordStream: createMpuUnlinkLiveRecordStream,
        createMpuMorayCleanerStream: createMpuMorayCleanerStream
};
