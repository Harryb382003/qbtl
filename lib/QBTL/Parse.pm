package QBTL::Parse;
use common::sense;

use TorrentParser;

sub run {
  my (%args) = @_;

  # Expect: all_torrents => arrayref of .torrent paths
  my $all_torrents = $args{all_torrents} || [];
  my $opts         = $args{opts}         || {};

  my $tp = TorrentParser->new({
    all_torrents => $all_torrents,
    opts         => $opts,
  });

  # For now, we don't pass qbt_loaded_tor (keep it simple / optional)
  my $parsed = $tp->extract_metadata( $args{qbt_loaded_tor} );

  return $parsed;
}

1;
