#!/usr/bin/env perl

###############################################################################
# Acts as a filter to output all the links that must be made for moray and
# mako.  This should go away post-haste after the stream to many mpipes
# is written.
###############################################################################

if (@ARGV < 3) {
    print "Usage: ".$ENV{"_"}." [manta_user] [output file] [manta object]\n";
    exit 1;
}
$user = $ARGV[0];
$file = $ARGV[1];
$object = $ARGV[2];

while($line = <STDIN>) {
    @parts = split(/\t/, $line);
    if ($parts[0] eq "mako") {
	$k{"mako"}{$parts[3]} = 1;
    }
    else {
	$k{"moray"}{$parts[1]} = 1;
    }
    #This is just a filter...
    print $line;
}

@kparts = split(/\//, $object);
$k = $kparts[-1];

open(OUT, ">$file");
for $node (sort keys %{ $k{"mako"} } ) {
    print OUT "mmkdir /$user/stor/manta_gc/mako/$node\n";
    print OUT "mln $object /$user/stor/manta_gc/mako/$node/$k\n";
}
for $shard (sort keys %{ $k{"moray"} } ) {
    print OUT "mmkdir /$user/stor/manta_gc/moray/$shard\n";
    print OUT "mln $object /$user/stor/manta_gc/moray/$shard/$k\n";
}
close(OUT);
