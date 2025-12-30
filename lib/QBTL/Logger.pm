package QBTL::Logger;

use common::sense;
use Exporter 'import';
use Time::HiRes qw(time);

our @EXPORT_OK = qw(
    debug
    info
    error
    set_log_file
    enable_disk_logging
);

our $LOG_FILE           = '';        # empty == disabled
our $DISK_LOGGING       = 0;         # off by default
our $DISK_LOGGING_LEVEL = 'debug';

sub summary { goto &debug }
sub trace   { goto &debug }
sub warn    { goto &debug }

sub set_log_file {
  my ( $path ) = @_;
  $LOG_FILE = $path // '';
  return 1;
}

sub enable_disk_logging {
  my ( %args ) = @_;
  $DISK_LOGGING = 1;

  # default: repo root ./qbtl.log
  $LOG_FILE = $args{path} // ( $LOG_FILE || 'qbtl.log' );

  # optional: level gate (debug/info/error)
  $DISK_LOGGING_LEVEL = $args{level} // $DISK_LOGGING_LEVEL;

  return 1;
}

sub debug { _emit( 'debug', @_ ); return 1; }
sub info  { _emit( 'info',  @_ ); return 1; }
sub error { _emit( 'error', @_ ); return 1; }

# ------------------------------
# internals
# ------------------------------

sub _emit {
  my ( $lvl, @msg ) = @_;
  my $s = join( '', map { defined $_ ? $_ : '' } @msg );

  # keep existing behavior (stdout) if you already had it
  # NOTE: Mojo may capture; disk logging is what we rely on for persistence.
  print STDOUT _fmt( $lvl, $s ) . "\n" if $ENV{QBTL_LOG_STDOUT};

  _log_to_disk( $lvl, $s );
  return 1;
}

sub _log_to_disk {
  my ( $lvl, $msg ) = @_;

  return unless $DISK_LOGGING;
  return unless $LOG_FILE;

  # level gate (simple)
  my %rank = ( debug => 1, info => 2, error => 3 );
  my $want = $rank{$DISK_LOGGING_LEVEL} // 1;
  my $have = $rank{$lvl}                // 1;
  return if $have < $want;

  my $line = _fmt( $lvl, $msg ) . "\n";

  # append; do not die on logging failure
  if ( open my $fh, '>>', $LOG_FILE ) {
    print $fh $line;
    close $fh;
  }

  return 1;
}

sub _fmt {
  my ( $lvl, $msg ) = @_;
  my $ts = scalar localtime();
  $msg =~ s/\s+\z// if defined $msg;
  return "[$ts] [$lvl] $msg";
}

1;
