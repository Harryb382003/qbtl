package QBTL::LocalCache;

use common::sense;

use JSON::PP    ();
use File::Temp  qw(tempfile);
use File::Slurp qw(read_file write_file);
use Storable    qw(nstore retrieve);

use QBTL::Scan;
use QBTL::Parse;

my %_MEMO;    # { path_key => { mtime => ..., data => ... } }

sub cache_path { cache_path_json( @_ ) }

sub cache_path_json {
  my ( %args ) = @_;
  my $root = $args{root_dir} || '.';
  return "$root/db/local_by_ih_cache.json";
}

sub cache_path_bin {
  my ( %args ) = @_;
  my $root = $args{root_dir} || '.';
  return "$root/db/local_by_ih_cache.stor";
}

sub _load_with_memo {
  my ( $path, $loader ) = @_;
  return undef unless -e $path;

  my $mtime = ( stat( $path ) )[9] || 0;

  if ( my $m = $_MEMO{$path} ) {
    return $m->{data} if ( $m->{mtime} || 0 ) == $mtime;
  }

  my $data = $loader->();
  $data = {} if ref( $data ) ne 'HASH';

  $_MEMO{$path} = {mtime => $mtime, data => $data};
  return $data;
}

sub load_local_by_ih {
  my ( %args ) = @_;

  my $bin  = cache_path_bin( %args );
  my $json = cache_path_json( %args );

  # Prefer binary if present
  if ( -e $bin ) {
    my $data = _load_with_memo( $bin, sub { retrieve( $bin ) } );
    return unless $data && ref( $data ) eq 'HASH';
    my $mtime = ( stat( $bin ) )[9] || 0;
    return ( $data, $mtime, $bin );
  }

  return unless -e $json;

  my $data = _load_with_memo(
    $json,
    sub {
      my $raw  = read_file( $json );
      my $data = JSON::PP::decode_json( $raw );
      return $data;
    } );

  return unless $data && ref( $data ) eq 'HASH';
  my $mtime = ( stat( $json ) )[9] || 0;
  return ( $data, $mtime, $json );
}

sub build_local_by_ih {
  my ( %args )   = @_;
  my $opts_local = $args{opts_local} || {};
  my $root_dir   = $args{root_dir}   || '.';

  my $scan = QBTL::Scan::run( opts => $opts_local );
  my $parsed = QBTL::Parse::run( all_torrents => $scan->{torrents},
                                 opts         => $opts_local );

  my $local_by_ih = $parsed->{by_infohash} || $parsed->{infohash_map} || {};
  $local_by_ih = {} if ref( $local_by_ih ) ne 'HASH';

  my $path_json = cache_path_json( root_dir => $root_dir );
  my $path_bin  = cache_path_bin( root_dir => $root_dir );

  # write binary first (fastest path for next restart)
  nstore( $local_by_ih, $path_bin );

  # write json for humans
  my $json =
      JSON::PP->new->utf8->pretty->canonical( 1 )->encode( $local_by_ih );
  my ( $fh, $tmp ) = tempfile( "$path_json.XXXX", UNLINK => 0 );
  print {$fh} $json;
  close $fh;
  rename $tmp, $path_json or die "rename($tmp → $path_json): $!";

  # refresh memo immediately (no next-request lag)
  my $mtime_bin  = ( stat( $path_bin ) )[9]  || time();
  my $mtime_json = ( stat( $path_json ) )[9] || time();

  $_MEMO{$path_bin}  = {mtime => $mtime_bin,  data => $local_by_ih};
  $_MEMO{$path_json} = {mtime => $mtime_json, data => $local_by_ih};

 # IMPORTANT: since you *prefer* .stor at load time, report THAT as "Last Cache"
  return ( $local_by_ih, $mtime_bin, $path_bin );
}

sub get_local_by_ih {
  my ( %args ) = @_;
  my ( $data, $mtime, $src ) = load_local_by_ih( %args );
  return ( $data, $mtime, $src ) if $data;

  return build_local_by_ih( %args );
}
1;
