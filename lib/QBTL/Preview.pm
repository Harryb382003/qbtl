package QBTL::Preview;

use common::sense;
use File::Temp qw(tempdir);
use File::Path qw(make_path);
use File::Spec;
use Encode qw(encode);

sub build_preview_dir_from_rec {
  my (%args) = @_;
  my $ih  = $args{ih}  // '';
  my $rec = $args{rec} // {};
  die "bad rec" unless ref($rec) eq 'HASH';

  my $name  = $rec->{name} // '';
  my $files = $rec->{files};
  $files = [] if ref($files) ne 'ARRAY';

  $name ||= $ih ? "torrent_$ih" : "torrent";

  my $dir = tempdir("qbtl_view_${ih}_XXXX", TMPDIR => 1, CLEANUP => 0);

  # Top-level folder like the torrent root
  my $top = File::Spec->catdir($dir, $name);
  make_path($top);

  if (@$files) {
    for my $f (@$files) {
      next unless ref($f) eq 'HASH';
      my $rel = $f->{path} // '';
      next unless length $rel;

      # torrent paths are POSIX-ish
      $rel =~ s{\\}{/}g;
      $rel =~ s{^\s+|\s+$}{}g;
      next unless length $rel;

      my @parts = split m{/+}, $rel;

      # ---- KEY FIX: if torrent-relative path starts with the torrent name,
      # strip it to avoid $name/$name duplication
      if (@parts && defined($parts[0]) && $parts[0] eq $name) {
        shift @parts;
      }

      # If stripping made it empty (weird edge case), skip
      next unless @parts;

      my $leaf = pop @parts;
      next unless defined $leaf && length $leaf;

      my $d = @parts ? File::Spec->catdir($top, @parts) : $top;
      make_path($d) unless -d $d;

      my $p = File::Spec->catfile($d, $leaf);

      # Full placeholders always: create a 0-byte file
      open my $fh, '>:raw', $p or next;
      close $fh;
    }
  } else {
    # fallback: make a single placeholder
    my $p = File::Spec->catfile($top, $name);
    open my $fh, '>:raw', $p or die "open($p): $!";
    close $fh;
  }

  return $dir;
}

sub open_in_finder {
  my ($path) = @_;
  return 0 unless defined $path && -d $path;
  system('open', $path);
  return 1;
}

1;
