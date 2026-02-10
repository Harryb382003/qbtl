# lib/QBTL/Store.pm
package QBTL::Store;

use common::sense;
use Scalar::Util qw(reftype);
use Hash::Util   qw(lock_hashref unlock_hashref lock_keys);
use Exporter 'import';
our @EXPORT_OK = qw( store_put_qbt_snapshot store_put_local_primary);

# ------------------------------------------------------------------------------
# Public API:
#   store_init($app)
#   store_put_local_primary($app, $local_by_ih, %opt)   # deep-locks primary
#   store_put_qbt_snapshot($app, $qbt_by_ih, %opt)      # deep-locks snapshot
#   store_put_local_override($app, $ih, \%ovr)
#   store_get_local_primary($app, $ih)
#   store_get_qbt($app, $ih)
#   store_effective_root($app, $ih)                     # example resolver
#
# Notes:
#   - Primary hashes are treated as immutable snapshots: we deep-lock them.
#   - “Overrides” / runtime / classes are mutable by design.
#   - “Only API can write qbt” is enforced by swapping the snapshot, not mutating.
# ------------------------------------------------------------------------------

sub store_init {
  my ( $app ) = @_;
  return undef unless $app;

  $app->defaults->{store} ||= {
    local_primary => {},    # { ih => { ...torrent meta... } }  (locked records)
    qbt => {
            by_ih => {},          # ih => locked qbt snapshot
            ts    => 0,
    },    # { ih => { ...qbt fields... } }    (locked records)
    local_overrides => {}, # { ih => { root_override => "...", ... } } (mutable)
    runtime         => {}, # { ih => { ... } } (mutable)
    classes         => {}, # { CLASS => { ih|id => { ... } } } (mutable)
  };

  return $app->defaults->{store};
}

sub store_put_local_primary {
  my ( $app, $local_by_ih, %opt ) = @_;
  return 0 unless $app && ref( $local_by_ih ) eq 'HASH';

  my $store = store_init( $app );

  # Build a fresh snapshot (do not mutate existing locked records)
  my %fresh;
  for my $ih ( keys %$local_by_ih ) {
    next unless defined $ih && $ih =~ /^[0-9a-f]{40}$/;
    my $rec = $local_by_ih->{$ih};
    next unless ref( $rec ) eq 'HASH';

   # Shallow copy so caller can keep mutating their copy without affecting store
    my %copy = ( %$rec, ih => $ih );
    my $href = \%copy;

    _deep_lock( $href ) unless $opt{no_lock};
    $fresh{$ih} = $href;
  }

  $store->{local_primary} = \%fresh;
  return 1;
}

sub store_put_qbt_snapshot {
  my ( $app, $qbt_by_ih, %opt ) = @_;
  return 0 unless $app && ref( $qbt_by_ih ) eq 'HASH';

  my $store = store_init( $app );

  # canonical place
  $store->{qbt} ||= {};
  $store->{qbt}{by_ih} ||= {};

  my %fresh;
  for my $ih ( keys %$qbt_by_ih ) {
    next unless defined $ih && $ih =~ /^[0-9a-f]{40}$/;

    my $rec = $qbt_by_ih->{$ih};
    next unless ref( $rec ) eq 'HASH';

    my %copy = ( %$rec );

    # normalize: guarantee both exist and agree
    $copy{hash} = $ih
        unless defined( $copy{hash} ) && $copy{hash} =~ /^[0-9a-f]{40}$/;
    $copy{ih} = $ih;    # your repo-wide rule: callers use {ih}

    my $href = \%copy;
    _deep_lock( $href ) unless $opt{no_lock};

    $fresh{$ih} = $href;
  }

  $store->{qbt}{by_ih}       = \%fresh;
  $store->{qbt}{snapshot_ts} = time;

  # ---- DEBUG: write qbt snapshot json (for manual inspection) ----
  if ( !$opt{no_dump_json} ) {
    eval {
      require JSON::PP;
      require File::Spec;

      my $root = $app->defaults->{root_dir} || '.';
      my $dir  = File::Spec->catdir( $root, '.sandbox' );
      mkdir $dir unless -d $dir;

      my $path = File::Spec->catfile( $dir, 'qbt_snapshot.json' );

      my $json = JSON::PP->new->utf8->pretty->canonical( 1 )
          ->encode( $store->{qbt}{by_ih} );

      open my $fh, '>', $path or die "open($path): $!";
      print {$fh} $json;
      close $fh;

      1;
    } or do {
      my $e = "$@";
      chomp $e;
      $app->log->debug( prefix_dbg() . " qbt snapshot json write failed: $e" );
    };
  }

  return 1;
}

sub store_put_local_override {
  my ( $app, $ih, $ovr ) = @_;
  return 0 unless $app && defined( $ih ) && $ih =~ /^[0-9a-f]{40}$/;
  return 0 unless ref( $ovr ) eq 'HASH';

  my $store = store_init( $app );

  $store->{local_overrides}{$ih} ||= {};
  my $dst = $store->{local_overrides}{$ih};

  # Mutable by design
  @{$dst}{keys %$ovr} = values %$ovr;

  return 1;
}

sub store_get_local_primary {
  my ( $app, $ih ) = @_;
  return undef unless $app && defined( $ih ) && $ih =~ /^[0-9a-f]{40}$/;
  my $st = store_init( $app );
  return $st->{local_primary}{$ih};
}

sub store_get_qbt {
  my ( $app, $ih ) = @_;
  return undef unless $app && defined( $ih ) && $ih =~ /^[0-9a-f]{40}$/;
  my $st = store_init( $app );
  return $st->{qbt_snapshot}{$ih};
}

# Example “effective” resolver (use whatever keys you standardize on)
sub store_effective_root {
  my ( $app, $ih ) = @_;
  return '' unless $app && defined( $ih ) && $ih =~ /^[0-9a-f]{40}$/;

  my $st  = store_init( $app );
  my $ovr = $st->{local_overrides}{$ih} || {};
  my $pri = $st->{local_primary}{$ih}   || {};

  # Prefer override if present; else fall back to primary
  return $ovr->{root_override} // $pri->{name} // '';
}

# ------------------------------------------------------------------------------
# Internal: deep lock a structure (hashes + arrays + nested)
#   - Prevents “back door” edits to nested refs like files->[0]{path}
# ------------------------------------------------------------------------------

sub _deep_lock {
  my ( $ref, $seen ) = @_;
  return 1 unless ref( $ref );

  $seen ||= {};
  my $addr = "$ref";    # stringified ref address is stable enough here
  return 1 if $seen->{$addr}++;

  my $t = reftype( $ref ) || '';
  if ( $t eq 'HASH' ) {

    # Recurse first so nested are locked too
    for my $v ( values %$ref ) {
      _deep_lock( $v, $seen ) if ref( $v );
    }
    lock_hashref( $ref );    # prevents any key/value mutation
    return 1;
  }
  if ( $t eq 'ARRAY' ) {
    for my $v ( @$ref ) {
      _deep_lock( $v, $seen ) if ref( $v );
    }

 # You can't "lock_array_ref" with Hash::Util; simplest is to lock via tie,
 # but that's overkill. Practically: if elements are hashes, they’re locked.
 # If you want to prevent push/pop entirely, say so and I’ll add tie-based lock.
    return 1;
  }

  # Other refs (SCALAR/CODE/etc): ignore
  return 1;
}

1;
