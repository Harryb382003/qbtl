#!/usr/bin/env perl
use common::sense;
use Getopt::Long;

my %opts;
GetOptions( 'dev-mode' => \$opts{dev_mode} );

$ENV{QBTL_DEV_MODE} = 1 if $opts{dev_mode};

while ( 1 ) {
  system( $^X, "bin/qbtl.pl", ( $opts{dev_mode} ? "--dev-mode" : () ) );
  my $exit = $? >> 8;
  last if $exit != 42;    # restart only when server exits 42
  warn "\n[qbtl-run] restart requested\n\n\n\n\n";
}
