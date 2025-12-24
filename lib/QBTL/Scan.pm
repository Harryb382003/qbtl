package QBTL::Scan;
use common::sense;

use FindBin qw($Bin);
use lib "$Bin/../lib";
use lib "$Bin/../lib/QBTL";

use TorrentParser qw(torrent_paths);

sub run {
  my (%args) = @_;
  my $opts = $args{opts} || {};

  my @torrents = torrent_paths($opts);

  return {status   => 'ok',
          found    => scalar(@torrents),
          torrents => \@torrents,};
}

1;
