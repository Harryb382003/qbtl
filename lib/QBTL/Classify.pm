# lib/QBTL/Classify.pm
package QBTL::Classify;

use common::sense;
use File::Basename qw(basename);
use Scalar::Util   qw(refaddr);
use QBTL::Utils    qw(prefix_dbg short_ih);

use Exporter 'import';
our @EXPORT_OK = qw(
    classify_no_hits
    classify_triage
    classify_volume_missing

);

# Centralized, in-memory classification store:
#   $app->defaults->{classes}{$CLASS}{$ih} = { ...payload..., ts=>time, why=>... }
# Runtime overlay for “already touched this server run”:
#   $app->defaults->{runtime}{processed}{$ih} = { status=>..., ts=>..., why=>... }
#
# Hard rules:
# - ih key is always 40 lowercase hex (we do NOT munge case; invalid ih => reject)
# - do not die; return 1/0
# - keep payload lightweight and predictable; store-time timestamp ("A")

sub classify_no_hits {
  my ( $app, $rec, $why ) = @_;
  return 0 unless $app && ref( $rec ) eq 'HASH';

  my $ih = $rec->{ih} // '';
  return 0 unless $ih =~ /^[0-9a-f]{40}$/;

  _ensure_class_bucket( $app, 'NO_PAYLOAD' );

  $app->defaults->{classes}{NO_PAYLOAD}{$ih} = {
    %$rec,
    ts       => time,
    why      => ( $why // 'no_hit' ),
    subclass => 'NO_HITS',
  };

  # Optional: treat NO_PAYLOAD as “processed” for this run so it vanishes from Page_View
  _mark_processed(
    $app, $ih,
    status => 'no_payload',
    why    => ( $why // 'no_hit' ),
  );

  return 1;
}

sub classify_triage {
  my ( $app, $rec, $why, %opt ) = @_;
  return 0 unless $app && ref( $rec ) eq 'HASH';

  my $ih     = $rec->{ih} // '';
  my $has_ih = ( defined($ih) && $ih =~ /^[0-9a-f]{40}$/ ) ? 1 : 0;

  # Hard rule: triage is keyed by ih. If missing -> caller bug bucket.
  if ( !$has_ih ) {
    my $bug = _ensure_class_bucket_path( $app, qw(TRIAGE_BUG) );

    my $id = join '-', time, $$, int( rand(1_000_000) );

    $bug->{$id} = {
      %$rec,
      ts  => time,
      why => ( $why // 'triage' ),
      bug => 'missing_ih',
    };

    $app->log->error(
      prefix_dbg()
        . " TRIAGE_BUG missing ih (caller lost key) id=$id why=["
        . ( $why // '' )
        . "] key=["
        . ( $rec->{key} // '' )
        . "] path=["
        . ( $rec->{path} // '' )
        . "]"
    );

    return 0;
  }

  my $h = _ensure_class_bucket_path( $app, qw(TRIAGE) );

  my $id = $ih;

  $h->{$id} = {
    %$rec,
    ts  => time,
    why => ( $why // 'triage' ),
  };

  _mark_processed(
    $app, $ih,
    status => ( $opt{status} // 'triage' ),
    why    => ( $why         // 'triage' ),
  );

  $app->log->debug(
    prefix_dbg()
      . " TRIAGE add ih="
      . short_ih($ih)
      . " why=["
      . ( $why // '' )
      . "]"
  );

  return 1;
}

sub classify_qbt_default_path {
  my ( $app, $rec, $why ) = @_;
  return 0 unless $app && ref( $rec ) eq 'HASH';

  my $ih = $rec->{ih} // '';
  return 0 unless $ih =~ /^[0-9a-f]{40}$/;

  _ensure_class_bucket( $app, 'QBT_DEFAULT_PATH' );

  $app->defaults->{classes}{QBT_DEFAULT_PATH}{$ih} = {
                                          %$rec,
                                          ts  => time,
                                          why => ( $why // 'qbt_default_path' ),
  };

  # optional: treat as processed so it vanishes from Page_View this run
  _mark_processed(
                   $app, $ih,
                   status => 'qbt_default_path',
                   why    => ( $why // 'qbt_default_path' ) );

  return 1;
}


sub classify_volume_missing {
  my ( $app, $rec, $why ) = @_;
  return 0 unless $app && ref( $rec ) eq 'HASH';

  my $ih = $rec->{ih} // '';
  return 0 unless $ih =~ /^[0-9a-f]{40}$/;

  _ensure_class_bucket( $app, 'NO_PAYLOAD' );

  $app->defaults->{classes}{NO_PAYLOAD}{$ih} = {
    %$rec,
    ts       => time,
    why      => ( $why // 'volume_missing' ),
    subclass => 'VOLUME_MISSING',
  };

  _mark_processed(
    $app, $ih,
    status => 'no_payload',
    why    => ( $why // 'volume_missing' ),
  );

  return 1;
}

sub classify_zero_bytes {
  my ( $app, $rec, $why ) = @_;
  return 0 unless $app && ref( $rec ) eq 'HASH';

  my $ih = $rec->{ih} // '';
  return 0 unless $ih =~ /^[0-9a-f]{40}$/;

  _ensure_class_bucket( $app, 'ZERO_BYTES' );

  $app->defaults->{classes}{ZERO_BYTES}{$ih} = {
                                                %$rec,
                                                ts  => time,
                                                why => ( $why // 'zero_bytes' ),
  };

  # optional: treat as processed so it vanishes from Page_View this run
  _mark_processed(
                   $app, $ih,
                   status => 'zero_bytes',
                   why    => ( $why // 'zero_bytes' ) );

  return 1;
}

# --------------------------
# Helpers (private)
# --------------------------

sub _ensure_class_bucket {
  my ( $app, $class ) = @_;
  $app->defaults->{classes} ||= {};
  $app->defaults->{classes}{$class} ||= {};
  return 1;
}

sub _ensure_class_bucket_path {
  my ( $app, @path ) = @_;
  $app->defaults->{classes} ||= {};

  my $h = $app->defaults->{classes};
  for my $k (@path) {
    $h->{$k} ||= {};
    $h = $h->{$k};
    $h = {} if ref($h) ne 'HASH'; # harden: if someone stomped it, reset
  }

  return $h;
}

sub _mark_processed {
  my ( $app, $ih, %meta ) = @_;
  return 0 unless $ih && $ih =~ /^[0-9a-f]{40}$/;

  $app->defaults->{runtime} ||= {};
  $app->defaults->{runtime}{processed} ||= {};

  $app->defaults->{runtime}{processed}{$ih} = {
    ts     => time,
    status => ( $meta{status} // 'processed' ),
    why    => ( $meta{why}    // '' ),
  };

  return 1;
}

sub _triage_id {
  my ( $app ) = @_;
  my $id      = join '-', time, $$, int( rand( 1_000_000 ) );
  $app->log->debug( prefix_dbg() . " TRIAGE generated id=$id" ) if $app;
  return $id;
}

1;
