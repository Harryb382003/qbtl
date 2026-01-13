#!/usr/bin/env perl
use common::sense;

my $VERSION = '0.0.1';

use Getopt::Long qw(:config bundling);
use FindBin      qw($Bin);
use utf8;
use open  qw(:std :encoding(UTF-8));

use lib "$Bin/../lib";
use lib "$Bin/../lib/QBTL/Legacy";
use QBTL::Web;

my %opts;
GetOptions( 'dev-mode' => \$opts{dev_mode} ) or die "Bad options\n";

my $root = "$Bin/..";
$opts{root_dir} = $root;

my $app = QBTL::Web::app( \%opts );
$app->start( 'daemon', '-l', 'http://127.0.0.1:8080' );
