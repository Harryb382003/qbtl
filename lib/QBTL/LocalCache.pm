package QBTL::LocalCache;

# lib/QBTL/LocalCache.pm
use common::sense;

use JSON::PP    ();
use File::Temp  qw(tempfile);
use File::Slurp qw(read_file write_file);
use Storable    qw(nstore retrieve);

use QBTL::Scan;
use QBTL::Store;
use QBTL::Parse;

# Public API:
#   get_local_by_ih
#   load_local_by_ih
#   build_local_by_ih
#   cache_path_bin
#   cache_path_json

# --- module globals ---
our %_MEMO;    # path -> { mtime => INT, data => HASHREF }
our $_BROKEN = {};

sub get_local_by_ih {
  my ( $app, %args ) = @_;
  my ( $data, $mtime, $src ) = load_local_by_ih( %args );
  return ( $data, $mtime, $src ) if $data && ref( $data ) eq 'HASH';

  return build_local_by_ih( $app, %args );
}

sub load_local_by_ih {
  my ( $app, %args ) = @_;

  my $bin  = cache_path_bin( %args );
  my $json = cache_path_json( %args );

  # Prefer binary if present (but never trust it blindly)
  if ( -e $bin ) {
    my $data = _load_with_memo( $bin, sub { retrieve( $bin ) },
                                %args, broken_key => 'bin', );

    if ( $data && ref( $data ) eq 'HASH' ) {
      my $mtime = ( stat( $bin ) )[9] || 0;
      return ( $data, $mtime, $bin );
    }

    # fall through to json/build
  }

  return unless -e $json;

  my $data = _load_with_memo(
    $json,
    sub {
      my $raw  = read_file( $json );
      my $data = JSON::PP::decode_json( $raw );
      return $data;
    },
    %args,
    broken_key => 'json', );

  return unless $data && ref( $data ) eq 'HASH';
  my $mtime = ( stat( $json ) )[9] || 0;
  return ( $data, $mtime, $json );
}

sub build_local_by_ih {
  my ( $app, %args ) = @_;
  my $opts_local = $args{opts_local} || {};
  my $root_dir   = $args{root_dir}   || '.';

  my $local_by_ih;

  my $ok = eval {
    my $scan = QBTL::Scan::run( opts => $opts_local );
    die "scan returned non-HASH" unless ref( $scan ) eq 'HASH';

    my $parsed = QBTL::Parse::run( all_torrents => $scan->{torrents},
                                   opts         => $opts_local );
    die "parse returned non-HASH" unless ref( $parsed ) eq 'HASH';

    $local_by_ih = $parsed->{by_infohash} || $parsed->{infohash_map} || {};
    die "parsed dataset not a HASH" unless ref( $local_by_ih ) eq 'HASH';

    1;
  };

  store_put_local_primary( $app, $local_by_ih );

  unless ( $ok ) {
    my $e = "$@";
    chomp $e;
    _mark_broken( %args, key => 'build', why => $e );

    # Policy: build failure returns EMPTY clean dataset (quality dataset),
    # but marked broken so UI/logs can show degraded mode.
    return ( {}, 0, 'build_failed' );
  }

  my $path_json = cache_path_json( root_dir => $root_dir );
  my $path_bin  = cache_path_bin( root_dir => $root_dir );

  # write binary first (fastest path for next restart)
  my $wok = eval {
    nstore( $local_by_ih, $path_bin );

    # write json for humans
    my $json =
        JSON::PP->new->utf8->pretty->canonical( 1 )->encode( $local_by_ih );
    my ( $fh, $tmp ) = tempfile( "$path_json.XXXX", UNLINK => 0 );
    print {$fh} $json;
    close $fh;
    rename $tmp, $path_json or die "rename($tmp -> $path_json): $!";

    1;
  };

  unless ( $wok ) {
    my $e = "$@";
    chomp $e;
    _mark_broken( %args, key => 'build', why => "cache write failed: $e" );

# Still return the in-memory dataset (it IS good); cache persistence is degraded.
    return ( $local_by_ih, time(), 'memory_only' );
  }

  # refresh memo immediately (no next-request lag)
  my $mtime_bin  = ( stat( $path_bin ) )[9]  || time();
  my $mtime_json = ( stat( $path_json ) )[9] || time();

  $_MEMO{$path_bin}  = {mtime => $mtime_bin,  data => $local_by_ih};
  $_MEMO{$path_json} = {mtime => $mtime_json, data => $local_by_ih};

  # IMPORTANT: since we *prefer* .stor at load time, report THAT as "Last Cache"
  return ( $local_by_ih, $mtime_bin, $path_bin );
}

sub _load_with_memo {
  my ( $path, $loader, %args ) = @_;
  return undef unless -e $path;

  my $mtime = ( stat( $path ) )[9] || 0;

  if ( my $m = $_MEMO{$path} ) {
    return $m->{data} if ( $m->{mtime} || 0 ) == $mtime;
  }

  my $data;
  my $ok = eval { $data = $loader->(); 1; };
  if ( !$ok ) {
    my $e = "$@";
    chomp $e;
    _mark_broken( %args, key => ( $args{broken_key} || 'load' ), why => $e );
    return undef;
  }

  unless ( ref( $data ) eq 'HASH' ) {
    _mark_broken(
        %args,
        key => ( $args{broken_key} || 'load' ),
        why => "loader returned non-HASH (" . ( ref( $data ) || 'SCALAR' ) . ")"
    );
    return undef;
  }

  $_MEMO{$path} = {mtime => $mtime, data => $data};
  return $data;
}

sub _mark_broken {
  my ( %args ) = @_;
  my $key      = $args{key} // 'unknown';
  my $why      = $args{why} // 'unknown';

  my $rec = {ts => time, why => $why};

  if ( my $app = $args{app} ) {

    # store broken flags centrally in defaults (preferred)
    $app->defaults->{local_cache} ||= {};
    $app->defaults->{local_cache}{broken} ||= {};
    $app->defaults->{local_cache}{broken}{$key} = $rec;
    return 1;
  }

  # fallback: module global
  $_BROKEN ||= {};
  $_BROKEN->{$key} = $rec;
  return 1;
}

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

1;
