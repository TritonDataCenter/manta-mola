#!/usr/bin/env perl

###############################################################################
# Acts as a filter to output all the links that must be made for moray and
# mako.  This should go away post-haste after the stream to many mpipes
# is written.
###############################################################################

if (@ARGV < 2) {
    print "Usage: ".$ENV{"_"}." [output file] [manta key]\n";
    exit 1;
}
$file = $ARGV[0];
$key = $ARGV[1];

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

@kparts = split(/\//, $key);
$k = $kparts[-1];

open(OUT, ">$file");
for $node (sort keys %{ $k{"mako"} } ) {
    print OUT "mmkdir /manta_gc/mako/$node\n";
    print OUT "mln $key /manta_gc/mako/$node/$k\n";
}
for $shard (sort keys %{ $k{"moray"} } ) {
    print OUT "mmkdir /manta_gc/moray/$shard\n";
    print OUT "mln $key /manta_gc/moray/$shard/$k\n";
}
close(OUT);
