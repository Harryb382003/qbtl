#!/usr/bin/env perl
use common::sense;

my $VERSION = '0.0.1';

use FindBin qw($Bin);

use lib "$Bin/../lib";
use lib "$Bin/../lib/QBTL/Legacy";

use QBTL::Web;

my $app = QBTL::Web::app();
$app->start('daemon', '-l', 'http://127.0.0.1:8080');
