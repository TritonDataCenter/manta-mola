/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var jsprim = require('jsprim');
var stream = require('stream');
var util = require('util');

var mpuCommon = require('../../lib/mpu/common');


///--- Globals

var MORAY_1 = '1.moray.coal.joyent.us';
var MORAY_2 = '2.moray.coal.joyent.us';


// Batch 0: Committed MPU with finalizing record, upload record, 1 part records
var ID_0 = '07cff761-33c7-c9ad-a9a0-d3303afa1490';
var DATE_0 =  new Date();
var SHARD_0 =  MORAY_1;
/* BEGIN JSSTYLED */
var KEY_FR0 = '07cff761-33c7-c9ad-a9a0-d3303afa1490:/4204a7f8-3d97-ec27-c16d-f2f49366cc3c/stor/batch0';
var KEY_UR0 = '/4204a7f8-3d97-ec27-c16d-f2f49366cc3c/uploads/0/07cff761-33c7-c9ad-a9a0-d3303afa1490';
var KEY_PR0_0 = '/4204a7f8-3d97-ec27-c16d-f2f49366cc3c/uploads/0/07cff761-33c7-c9ad-a9a0-d3303afa1490/0';
/* END JSSTYLED */
var FR_0 = [ ID_0, '0_finalizingRecord', DATE_0, SHARD_0, 'commit', KEY_FR0 ]
        .join('\t');
var UR_0 = [ ID_0, '1_uploadRecord', DATE_0, KEY_UR0 ].join('\t');
var PR_0 = [ [ ID_0, '2_partRecord', DATE_0, KEY_PR0_0 ].join('\t') ];
var OBJ_FR0 = new mpuCommon.FinalizingRecord({
        uploadId: ID_0,
        key: KEY_FR0,
        shard: SHARD_0,
        date: DATE_0,
        type: 'commit'
});
assert.object(OBJ_FR0, 'failed to create test finalizing record 0 obj');
var OBJ_UR0 = new mpuCommon.LiveRecord({
        uploadId: ID_0,
        key: KEY_UR0,
        date: DATE_0,
        type: 'uploadRecord'
});
assert.object(OBJ_UR0, 'failed to create test upload record 0 obj');
var OBJ_PR0 = [
        new mpuCommon.LiveRecord({
                uploadId: ID_0,
                key: KEY_PR0_0,
                date: DATE_0,
                type: 'partRecord'
        })
];
OBJ_PR0.forEach(function (o) {
        assert.object(o, 'failed to create test part record 0 obj');
});
var ACCT_ID_0 = '4204a7f8-3d97-ec27-c16d-f2f49366cc3c';
var ACCT_LOGIN_0 = 'testuser0';
var PATH_UR0 = '/' + ACCT_LOGIN_0 +
        '/uploads/0/07cff761-33c7-c9ad-a9a0-d3303afa1490';
var PATH_PR0 = [
        '/' + ACCT_LOGIN_0 + '/uploads/0/07cff761-33c7-c9ad-a9a0-d3303afa1490/0'
];


// Batch 1: Aborted MPU with finalizing record, upload record, 3 part records
var ACCT_ID_1 = 'fdfe27dc-64bc-11e6-90f8-47c1ceb05dd8';
var ACCT_LOGIN_1 = 'testuser1';
var PATH_UR1 = '/' + ACCT_LOGIN_1 +
        '/uploads/c/c46e3e66-4311-6a11-8cf9-8d3fa69aaf0a';
var PATH_PR1 = [
        '/' + ACCT_LOGIN_1 +
                '/uploads/c/c46e3e66-4311-6a11-8cf9-8d3fa69aaf0a/0',
        '/' + ACCT_LOGIN_1 +
                '/uploads/c/c46e3e66-4311-6a11-8cf9-8d3fa69aaf0a/1',
        '/' + ACCT_LOGIN_1 +
                '/uploads/c/c46e3e66-4311-6a11-8cf9-8d3fa69aaf0a/2'
];

var ID_1 = 'c46e3e66-4311-6a11-8cf9-8d3fa69aaf0a';
var DATE_1 =  new Date();
var SHARD_1 =  MORAY_1;
/* BEGIN JSSTYLED */
var KEY_FR1 = 'c46e3e66-4311-6a11-8cf9-8d3fa69aaf0a:/fdfe27dc-64bc-11e6-90f8-47c1ceb05dd8/stor/batch1';
var KEY_UR1 = '/fdfe27dc-64bc-11e6-90f8-47c1ceb05dd8/uploads/c/c46e3e66-4311-6a11-8cf9-8d3fa69aaf0a';
var KEY_PR1_0 = '/fdfe27dc-64bc-11e6-90f8-47c1ceb05dd8/uploads/c/c46e3e66-4311-6a11-8cf9-8d3fa69aaf0a/0';
var KEY_PR1_1 = '/fdfe27dc-64bc-11e6-90f8-47c1ceb05dd8/uploads/c/c46e3e66-4311-6a11-8cf9-8d3fa69aaf0a/1';
var KEY_PR1_2 = '/fdfe27dc-64bc-11e6-90f8-47c1ceb05dd8/uploads/c/c46e3e66-4311-6a11-8cf9-8d3fa69aaf0a/2';
/* END JSSTYLED */
var FR_1 = [ ID_1, '0_finalizingRecord', DATE_1, SHARD_1, 'abort', KEY_FR1 ]
        .join('\t');
var UR_1 = [ ID_1, '1_uploadRecord', DATE_1, KEY_UR1 ].join('\t');
var PR_1 = [ [ ID_1, '2_partRecord', DATE_1, KEY_PR1_0 ].join('\t'),
             [ ID_1, '2_partRecord', DATE_1, KEY_PR1_1 ].join('\t'),
             [ ID_1, '2_partRecord', DATE_1, KEY_PR1_2 ].join('\t')
];
var OBJ_FR1 = new mpuCommon.FinalizingRecord({
        uploadId: ID_1,
        key: KEY_FR1,
        shard: SHARD_1,
        date: DATE_1,
        type: 'abort'
});
assert.object(OBJ_FR1, 'failed to create test finalizing record 1 obj');
var OBJ_UR1 = new mpuCommon.LiveRecord({
        uploadId: ID_1,
        key: KEY_UR1,
        date: DATE_1,
        type: 'uploadRecord'
});
assert.object(OBJ_UR1, 'failed to create test upload record 1 obj');
var OBJ_PR1 = [
        new mpuCommon.LiveRecord({
                uploadId: ID_1,
                key: KEY_PR1_0,
                date: DATE_1,
                type: 'partRecord'
        }),
        new mpuCommon.LiveRecord({
                uploadId: ID_1,
                key: KEY_PR1_1,
                date: DATE_1,
                type: 'partRecord'
        }),
        new mpuCommon.LiveRecord({
                uploadId: ID_1,
                key: KEY_PR1_2,
                date: DATE_1,
                type: 'partRecord'
        })
];
OBJ_PR1.forEach(function (o) {
        assert.object(o, 'failed to create test part record 1 objs');
});


// Batch 2: Committed MPU with finalizing record, upload record
var ACCT_ID_2 = '88af09d7-4845-e09a-8998-d7d04a88b879';
var ACCT_LOGIN_2 = 'testuser2';
var PATH_UR2 = '/' + ACCT_LOGIN_2 +
        '/uploads/3/38aecc30-9a8c-63a4-f906-e512f02f5915';

var ID_2 = '38aecc30-9a8c-63a4-f906-e512f02f5915';
var DATE_2 =  new Date();
var SHARD_2 =  MORAY_2;
/* BEGIN JSSTYLED */
var KEY_FR2 = '38aecc30-9a8c-63a4-f906-e512f02f5915:/88af09d7-4845-e09a-8998-d7d04a88b879/stor/batch2';
var KEY_UR2 = '/88af09d7-4845-e09a-8998-d7d04a88b879/uploads/3/38aecc30-9a8c-63a4-f906-e512f02f5915';
/* END JSSTYLED */
var FR_2 = [ ID_2, '0_finalizingRecord', DATE_2, SHARD_2, 'abort', KEY_FR2 ]
        .join('\t');
var UR_2 = [ ID_2, '1_uploadRecord', DATE_2, KEY_UR2 ].join('\t');
var OBJ_FR2 = new mpuCommon.FinalizingRecord({
        uploadId: ID_2,
        key: KEY_FR2,
        shard: SHARD_2,
        date: DATE_2,
        type: 'abort'
});
assert.object(OBJ_FR2, 'failed to create test finalizing record 2 obj');
var OBJ_UR2 = new mpuCommon.LiveRecord({
        uploadId: ID_2,
        key: KEY_UR2,
        date: DATE_2,
        type: 'uploadRecord'
});
assert.object(OBJ_UR2, 'failed to create test upload record 2 obj');


// Batch 3: Committed MPU with finalizing record, upload record, 1 part records,
// and account uuid different from object path account
var ID_3 = 'b3d58ef4-2277-4248-8cf0-c4243c4b0f83';
var DATE_3 =  new Date();
var SHARD_3 =  MORAY_2;
/* BEGIN JSSTYLED */
var KEY_FR3 = ID_3 + ':/1293dc6e-05a3-4651-baa3-1f74932f81b3/stor/batch3';
var KEY_UR3 = '/4204a7f8-3d97-ec27-c16d-f2f49366cc3c/uploads/b/b3d58ef4-2277-4248-8cf0-c4243c4b0f83';
var KEY_PR3_0 = '/4204a7f8-3d97-ec27-c16d-f2f49366cc3c/uploads/b/b3d58ef4-2277-4248-8cf0-c4243c4b0f83/0';
/* END JSSTYLED */
var FR_3 = [ ID_3, '0_finalizingRecord', DATE_3, SHARD_3, 'commit', KEY_FR3 ]
        .join('\t');
var UR_3 = [ ID_3, '1_uploadRecord', DATE_3, KEY_UR3 ].join('\t');
var PR_3 = [ [ ID_3, '2_partRecord', DATE_3, KEY_PR3_0 ].join('\t') ];
var OBJ_FR3 = new mpuCommon.FinalizingRecord({
        uploadId: ID_3,
        key: KEY_FR3,
        shard: SHARD_3,
        date: DATE_3,
        type: 'commit'
});
assert.object(OBJ_FR3, 'failed to create test finalizing record 3 obj');
var OBJ_UR3 = new mpuCommon.LiveRecord({
        uploadId: ID_3,
        key: KEY_UR3,
        date: DATE_3,
        type: 'uploadRecord'
});
assert.object(OBJ_UR3, 'failed to create test upload record 3 obj');
var OBJ_PR3 = [
        new mpuCommon.LiveRecord({
                uploadId: ID_3,
                key: KEY_PR3_0,
                date: DATE_3,
                type: 'partRecord'
        })
];
OBJ_PR3.forEach(function (o) {
        assert.object(o, 'failed to create test part record 3 obj');
});
var ACCT_ID_3 = '4204a7f8-3d97-ec27-c16d-f2f49366cc3c';
var ACCT_LOGIN_3 = 'testuser0';
var PATH_UR3 = '/' + ACCT_LOGIN_3 +
        '/uploads/b/b3d58ef4-2277-4248-8cf0-c4243c4b0f83';
var PATH_PR3 = [
        '/' + ACCT_LOGIN_3 + '/uploads/b/b3d58ef4-2277-4248-8cf0-c4243c4b0f83/0'
];



function ValidationStream(args) {
        assert.object(args, 'args');
        assert.func(args.cb, 'args.cb');
        assert.array(args.expect, 'args.expect');

        stream.Writable.call(this, {
            objectMode: true,
            highWaterMark: 0
        });

        var self = this;
        self.vs_received = [];

        self._write = function _write(chunk, _, cb) {
                self.vs_received.push(chunk);
                cb();
        };

        self.on('finish', function onFinish() {
                var ok = jsprim.deepEqual(self.vs_received, args.expect);
                args.cb(ok, self.vs_received);
        });
}
util.inherits(ValidationStream, stream.Writable);


module.exports = {
        ID_0: ID_0,
        DATE_0: DATE_0,
        SHARD_0: SHARD_0,
        KEY_FR0: KEY_FR0,
        KEY_UR0: KEY_UR0,
        KEY_PR0_0: KEY_PR0_0,
        FR_0: FR_0,
        UR_0: UR_0,
        PR_0: PR_0,
        OBJ_FR0: OBJ_FR0,
        OBJ_UR0: OBJ_UR0,
        OBJ_PR0: OBJ_PR0,
        ACCT_ID_0: ACCT_ID_0,
        ACCT_LOGIN_0: ACCT_LOGIN_0,
        PATH_UR0: PATH_UR0,
        PATH_PR0: PATH_PR0,

        ID_1: ID_1,
        DATE_1: DATE_1,
        SHARD_1: SHARD_1,
        KEY_FR1: KEY_FR1,
        KEY_UR1: KEY_UR1,
        KEY_PR1_0: KEY_PR1_0,
        KEY_PR1_1: KEY_PR1_1,
        KEY_PR1_2: KEY_PR1_2,
        FR_1: FR_1,
        UR_1: UR_1,
        PR_1: PR_1,
        OBJ_FR1: OBJ_FR1,
        OBJ_UR1: OBJ_UR1,
        OBJ_PR1: OBJ_PR1,
        ACCT_ID_1: ACCT_ID_1,
        ACCT_LOGIN_1: ACCT_LOGIN_1,
        PATH_UR1: PATH_UR1,
        PATH_PR1: PATH_PR1,

        ID_2: ID_2,
        DATE_2: DATE_2,
        SHARD_2: SHARD_2,
        KEY_FR2: KEY_FR2,
        KEY_UR2: KEY_UR2,
        FR_2: FR_2,
        UR_2: UR_2,
        OBJ_FR2: OBJ_FR2,
        OBJ_UR2: OBJ_UR2,
        ACCT_ID_2: ACCT_ID_2,
        ACCT_LOGIN_2: ACCT_LOGIN_2,
        PATH_UR2: PATH_UR2,

        ID_3: ID_3,
        DATE_3: DATE_3,
        SHARD_3: SHARD_3,
        KEY_FR3: KEY_FR3,
        KEY_UR3: KEY_UR3,
        KEY_PR3_0: KEY_PR3_0,
        FR_3: FR_3,
        UR_3: UR_3,
        PR_3: PR_3,
        OBJ_FR3: OBJ_FR3,
        OBJ_UR3: OBJ_UR3,
        OBJ_PR3: OBJ_PR3,
        ACCT_ID_3: ACCT_ID_3,
        ACCT_LOGIN_3: ACCT_LOGIN_3,
        PATH_UR3: PATH_UR3,
        PATH_PR3: PATH_PR3,

        ValidationStream: ValidationStream
};
