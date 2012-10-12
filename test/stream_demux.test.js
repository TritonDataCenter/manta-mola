//Copyright 2012 Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var fs = require('fs');
var helper = require('./helper.js');
var MemoryStream = require('memorystream');
var lib = require('../lib');



///--- Globals

var test = helper.test;



///--- Helpers

function testDualInMemory(opts, cb) {
        var data = opts.data;
        var keyFromLine = opts.keyFromLine;
        var selectStream = opts.selectStream;

        var input = new MemoryStream(data);
        input.pause();
        var out1 = new MemoryStream();
        var out2 = new MemoryStream();
        var out1Ended = false;
        var out2Ended = false;
        var od1 = '';
        var od2 = '';

        var demuxopts = {
                input: input,
                output: [out1, out2],
                keyFromLine: keyFromLine,
                selectStream: selectStream
        };

        lib.createStreamDemux(demuxopts, function (err) {
                cb(err, {
                        outputData1: od1,
                        outputData2: od2,
                        output1Ended: out1Ended,
                        output2Ended: out2Ended
                });
        });

        out1.on('data', function (d) {
                od1 += d;
        });
        out1.on('end', function () {
                out1Ended = true;
        });

        out2.on('data', function (d) {
                od2 += d;
        });
        out2.on('end', function () {
                out2Ended = true;
        });

        process.nextTick(function () {
                input.resume();
                input.end();
        });
}



///--- Tests

test('test: vanilla stream demux in memory', function (t) {
        var opts = {
                data: 'a\nb\nc\nd\ne'
        };
        testDualInMemory(opts, function (err, res) {
                if (err) {
                        assert.fail(err);
                }
                //If the hashing algorithm changes, these will need
                // to change as well.
                assert.equal('b\nc\n', res.outputData1);
                assert.equal('a\nd\ne\n', res.outputData2);
                t.end();
        });
});

test('test: stream demux, custom key selector', function (t) {
        var data = 'a|1\nb|2\nc|3\nd|4\ne|5';
        var keyFromLine = function (line) {
                var parts = line.split('|');
                return (parts[0]);
        };
        var opts = {
                data: data,
                keyFromLine: keyFromLine
        };
        testDualInMemory(opts, function (err, res) {
                if (err) {
                        assert.fail(err);
                }
                //If the hashing algorithm changes, these will need
                // to change as well.  Note that they are in the
                // same order as the vanilla test.
                assert.equal('b|2\nc|3\n', res.outputData1);
                assert.equal('a|1\nd|4\ne|5\n', res.outputData2);
                t.end();
        });
});

test('test: stream demux, vowels', function (t) {
        var data = 'a|1\nb|2\nc|3\nd|4\ne|5';
        //Yes, 'y' is sometimes a vowel as well.
        var vowels = ['a', 'e', 'i', 'o', 'u'];
        var keyFromLine = function (line) {
                var parts = line.split('|');
                return (parts[0]);
        };
        var selectStream = function (key, streams) {
                if (vowels.indexOf(key) != -1) {
                        return (streams[0]);
                }
                return (streams[1]);
        };
        var opts = {
                data: data,
                keyFromLine: keyFromLine,
                selectStream: selectStream
        };
        testDualInMemory(opts, function (err, res) {
                if (err) {
                        assert.fail(err);
                }
                //If the hashing algorithm changes, these will need
                // to change as well.  Note that they are in the
                // same order as the vanilla test.
                assert.equal('a|1\ne|5\n', res.outputData1);
                assert.equal('b|2\nc|3\nd|4\n', res.outputData2);
                t.end();
        });
});

test('test: file demux, evens and odds', function (t) {
        var fileName = './data/stream_demux/numbers.txt';
        var readStream = fs.createReadStream(fileName, { encoding: 'ascii' });
        readStream.pause();
        var evensName = './tmp/evens.txt';
        var oddsName = './tmp/odds.txt';
        var readStreamReady = false;
        var evensReady = false;
        var oddsReady = false;
        var evens = fs.createWriteStream(evensName, { encoding: 'ascii' });
        var odds = fs.createWriteStream(oddsName, { encoding: 'ascii' });
        var demuxDone = false;
        var oddsClosed = false;
        var evensClosed = false;

        var selectStream = function (key, streams) {
                var n = parseInt(key, 10);
                if ((n % 2) === 0) {
                        return (evens);
                }
                return (odds);
        };
        var opts = {
                input: readStream,
                output: [evens, odds],
                selectStream: selectStream
        };

        function checkOutputFiles() {
                var checkedEvens = false;
                var checkedOdds = false;

                function done() {
                        if (checkedEvens && checkedOdds) {
                                t.end();
                        }
                }

                fs.readFile(evensName, 'ascii', function (err, data) {
                        assert.equal('4\n22\n62\n68\n', data);
                        checkedEvens = true;
                        done();
                });

                fs.readFile(oddsName, 'ascii', function (err, data) {
                        assert.equal('3\n7\n15\n23\n97\n', data);
                        checkedOdds = true;
                        done();
                });
        }

        function resultsWhenDone() {
                if (demuxDone && evensClosed && oddsClosed) {
                        checkOutputFiles();
                }
        }

        function cont() {
                if (readStreamReady && evensReady && oddsReady) {
                        lib.createStreamDemux(opts, function (err) {
                                demuxDone = true;
                                resultsWhenDone();
                        });
                        readStream.resume();
                }
        }

        readStream.once('open', function () {
                readStreamReady = true;
                cont();
        });

        evens.once('open', function () {
                evensReady = true;
                cont();
        });

        evens.once('close', function () {
                evensClosed = true;
                resultsWhenDone();
        });

        odds.once('open', function () {
                oddsReady = true;
                cont();
        });

        odds.once('close', function () {
                oddsClosed = true;
                resultsWhenDone();
        });
});
