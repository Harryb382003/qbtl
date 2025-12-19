package QBTL::QBT;
use common::sense;

use QBittorrent;

sub infohash_set {
  my (%args) = @_;
  my $opts = $args{opts} || {};

  my $qb = QBittorrent->new($opts);
  my $set = $qb->get_torrents_infohash;   # legacy returns whatever it returns

  return $set;
}

1;
