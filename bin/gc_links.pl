#!/usr/bin/env perl
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

###############################################################################
# Acts as a filter to output all the links that must be made for moray and
# mako.  This should go away post-haste after the stream to many mpipes
# is written.
###############################################################################

if (@ARGV < 3) {
    print "Usage: ".$ENV{"_"}." [manta_user] [output file] " +
        "[manta object prefix]\n";
    exit 1;
}
$user = $ARGV[0];
$file = $ARGV[1];
$prefix = $ARGV[2];

while($line = <STDIN>) {
    @parts = split(/\t/, $line);
    #Parts 0 will be either "mako" or "moray"
    #Parts 1 will be the node or shard id
    $k{$parts[0]}{$parts[1]} = 1;
    print $line;
}

open(OUT, ">$file");
for $node (sort keys %{ $k{"mako"} } ) {
    $object = "$prefix-mako-$node";
    $k = (split(/\//, $object))[-1];
    print OUT "mmkdir /$user/stor/manta_gc/mako/$node\n";
    print OUT "mln $object /$user/stor/manta_gc/mako/$node/$k\n";
}
for $shard (sort keys %{ $k{"moray"} } ) {
    $object = "$prefix-moray-$shard";
    $k = (split(/\//, $object))[-1];
    print OUT "mmkdir /$user/stor/manta_gc/moray/$shard\n";
    print OUT "mln $prefix-moray-$shard /$user/stor/manta_gc/moray/$shard/$k\n";
}
close(OUT);
