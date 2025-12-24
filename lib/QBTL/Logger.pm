package Logger;

use common::sense;
use Data::Dumper;

use Term::ANSIColor;
use File::Spec;
use File::Path qw(make_path);

use lib 'lib';

my $log_fh;
my $log_file;
my $verbose = 0;
our $opts = {};

# Buffers
my @SUMMARY_BUFFER;

# Define two palettes
my %color_scheme_light = (INFO    => 'white',
                          WARN    => 'yellow',
                          ERROR   => 'red',
                          DEBUG   => 'cyan',
                          TRACE   => 'magenta',
                          SUCCESS => 'green',
                          DEV     => 'blue',);

my %color_scheme_dark = (INFO    => 'bright_white',
                         WARN    => 'bright_yellow',
                         ERROR   => 'bright_red',
                         DEBUG   => 'bright_cyan',
                         TRACE   => 'bright_magenta',
                         SUCCESS => 'bright_green',
                         DEV     => 'bright_blue',);

my %active_colors;
my $defer_blank = 0;

# sub init {
#     my ($init_opts) = @_;
#     # store caller’s opts in our package-global
#     $opts = $init_opts;
#
#     # pick color scheme
#     if ($opts->{dark_mode}) {
#         %active_colors = %color_scheme_dark;
#     } else {
#         %active_colors = %color_scheme_light;
#     }
#     if ($opts->{invert_colors}) {
#         %active_colors = $opts->{dark_mode} ? %color_scheme_light : %color_scheme_dark;
#     }
#
#     # logfile
#     my $log_dir = $opts->{log_dir} || '.';
#     make_path($log_dir) unless -d $log_dir;
#     $log_file = File::Spec->catfile($log_dir, "last.log");
#     open($log_fh, '>', $log_file) or die "Cannot open log file $log_file: $!";
# }
#
sub _log {

  # ensure a palette even if init() not called yet
  if (!%active_colors) { %active_colors = %color_scheme_light; }

  my ($level, @messages) = @_;
  my $timestamp = scalar localtime;

  my $color = $active_colors{$level} // 'reset';

  # --- verbosity gates (centralized) ---
  # Getopt 'v+' yields: -v=1, -vv=2, -vvv=3, -vvvv=4
  my $v = ($opts && defined $opts->{verbose}) ? $opts->{verbose} : 0;

  # DEBUG only with -vvv or higher
  if ($level eq 'DEBUG' && $v < 3)
  {
    return;
  }

  # TRACE only with -vvvv
  if ($level eq 'TRACE' && $v < 4)
  {
    return;
  }

  my $max_lines = 10;
  for my $msg (@messages)
  {
    $msg //= '';
    my $defer_blank = 0;

    while ($msg =~ s/^\n//) { print "\n"; print $log_fh "\n" if $log_fh; }
    while ($msg =~ s/\n$//) { $defer_blank = 1; }

    if ($msg eq '') { print "\n"; print $log_fh "\n" if $log_fh; next; }

    my ($console_msg, $file_msg) = ($msg, $msg);
    if (ref $msg)
    {
      local $Data::Dumper::Terse    = 1;
      local $Data::Dumper::Indent   = 1;
      local $Data::Dumper::Sortkeys = 1;
      $console_msg = $file_msg = Dumper($msg);

      if ($v < 3)
      {
        my @lines = split /\n/, $console_msg;
        if (@lines > $max_lines)
        {
          $console_msg =
              join("\n", @lines[0 .. $max_lines - 1]) . "\n... (truncated)";
        }
      }
    }

    print color($color) . "[$level] $console_msg" . color('reset') . "\n";
    print $log_fh "[$timestamp] [$level] $file_msg\n" if $log_fh;

    if ($defer_blank) { print "\n"; print $log_fh "\n" if $log_fh; }
  }
}
#
# sub _timestamp {
#     Logger::debug("#	_timestamp");
#     my @t = localtime();
#     return sprintf("%02d-%02d-%02d %02d:%02d:%02d",
#         $t[5]+1900, $t[4]+1, $t[3], $t[2], $t[1], $t[0]);
# }
#
# # Public logging methods
sub info    { _log("INFO",    @_); }
sub warn    { _log("WARN",    @_); }
sub error   { _log("ERROR",   @_); }
sub debug   { _log("DEBUG",   @_); }    #   if $verbose >= 2; }
sub trace   { _log("TRACE",   @_); }    #   if $verbose >= 3; }
sub success { _log("SUCCESS", @_); }
sub dev     { _log("DEV",     @_); }

# #     Logger::debug("#	dev");
# #     my ($msg) = @_;
# #     return unless $opts->{dev_mode};
# #     info("[DEV] $msg");
# # }
#
# # Log used CLI options
# sub log_used_opts {
#     Logger::debug("#	log_used_opts");
#     my ($used) = @_;
#     return unless $used && ref $used eq 'HASH';
#
#     Logger::info("[CONFIG] CLI options used:");
#     foreach my $key (sort keys %$used) {
#         my $val = $used->{$key};
#
#         if (ref $val eq 'ARRAY') {
#             Logger::info(sprintf("  %-15s :", $key));
#             foreach my $item (@$val) {
#                 Logger::info(sprintf("  %-15s   %s", '', $item));
#             }
#         }
#         elsif (ref $val eq 'HASH') {
#             Logger::info(sprintf("  %-15s :", $key));
#             foreach my $subkey (sort keys %$val) {
#                 Logger::info(sprintf("  %-15s   %s => %s", '', $subkey, $val->{$subkey}));
#             }
#         }
#         else {
#             my $formatted_val = ($val eq '1') ? 'yes' :
#                                 ($val eq '0') ? 'no'  : $val;
#             Logger::info(sprintf("  %-15s : %s", $key, $formatted_val));
#         }
#     }
# }
#

# --- Summary Buffer ---
sub summary {
  my ($msg) = @_;
  push @SUMMARY_BUFFER, $msg;
}

sub flush_summary {
  return unless @SUMMARY_BUFFER;

  print "\n--- Summary ---\n";
  foreach my $line (@SUMMARY_BUFFER)
  {
    if ($line =~ /^\n+/)
    {
      print "$line\n";
    }
    else
    {
      print "[SUMMARY] $line\n";
    }

  }
  @SUMMARY_BUFFER = ();
}

# sub raw {
#     my ($msg) = @_;
#     print $msg, "\n";  # bypass preambles completely
# }

1;
