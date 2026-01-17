# lib/QBTL/Classify.pm
package QBTL::Classify;

use common::sense;
use File::Basename qw(basename);
use Scalar::Util   qw(refaddr);
use QBTL::Utils    qw(prefix_dbg);

use Exporter 'import';
our @EXPORT_OK = qw(
    classify_no_hits
    classify_triage
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

  _ensure_class_bucket( $app, 'NO_HITS' );

  $app->defaults->{classes}{NO_HITS}{$ih} = {
                                             %$rec,
                                             ts  => time,
                                             why => ( $why // 'no_hit' ),};

# Optional: treat NO_HITS as “processed” for this run so it vanishes from Page_View
  _mark_processed(
                   $app, $ih,
                   status => 'no_hits',
                   why    => ( $why // 'no_hit' ) );

  return 1;
}

sub classify_triage {
  my ( $app, $rec, $why, %opt ) = @_;
  $app->log->debug(
                  prefix_dbg() . " classify_triage app_id=" . refaddr( $app ) );
  return 0 unless $app && ref( $rec ) eq 'HASH';

  my $ih = $rec->{ih} // '';
  if ( length( $ih ) && $ih !~ /^[0-9a-f]{40}$/ ) {

    # do not munge; if caller hands us junk, we refuse to store under a fake key
    $ih = '';
  }

  _ensure_class_bucket( $app, 'TRIAGE' );

  # TRIAGE record id:
  # - if ih exists, key directly by ih (stable, easy lookup)
  # - else, key by a generated per-run id (still visible in /qbt/triage)
  my $id = length( $ih ) ? $ih : _triage_id();

  $app->defaults->{classes}{TRIAGE}{$id} = {
                                            %$rec,
                                            ts  => time,
                                            why => ( $why // 'triage' ),};

  # Don’t spin: only mark “processed” when we have a real ih.
  if ( length( $ih ) ) {
    _mark_processed(
                     $app, $ih,
                     status => ( $opt{status} // 'triage' ),
                     why    => ( $why         // 'triage' ), );
  }
  $app->log->debug(   prefix_dbg()
                    . " TRIAGE add id=$id ih=["
                    . ( $rec->{ih} // '' )
                    . "] why=["
                    . ( $why // '' )
                    . "]" );
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

sub _mark_processed {
  my ( $app, $ih, %meta ) = @_;
  return 0 unless $ih && $ih =~ /^[0-9a-f]{40}$/;

  $app->defaults->{runtime} ||= {};
  $app->defaults->{runtime}{processed} ||= {};

  $app->defaults->{runtime}{processed}{$ih} = {
                                     ts     => time,
                                     status => ( $meta{status} // 'processed' ),
                                     why    => ( $meta{why}    // '' ),};

  return 1;
}

sub _triage_id {
  my ( $app ) = @_;
  my $id      = join '-', time, $$, int( rand( 1_000_000 ) );
  $app->log->debug( prefix_dbg() . " TRIAGE generated id=$id" ) if $app;
  return $id;
}

1;
