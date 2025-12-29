package QBTL::Scan;
use common::sense;

use QBTL::TorrentParser;

sub run {
  my ( %args ) = @_;
  my $opts = $args{opts} || {};

  my @torrents = QBTL::TorrentParser::torrent_paths( $opts );

  return {
          status   => 'ok',
          found    => scalar( @torrents ),
          torrents => \@torrents,};
}

1;
