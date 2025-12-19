package QBTL::Scan;
use common::sense;

use TorrentParser qw(locate_torrents);

sub run {
  my (%args) = @_;
  my $opts = $args{opts} || {};

  my @torrents = locate_torrents($opts);

  return {
    status  => 'ok',
    found   => scalar(@torrents),
    torrents => \@torrents,
  };
}

1;
