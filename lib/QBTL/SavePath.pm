package QBTL::SavePath;
use common::sense;

use File::Basename qw(dirname);
use Utils ();

sub derive_savepath_from_payload {
  my (%args) = @_;
  my $rec = $args{rec} || {};

  return (undef, "bad rec") if ref($rec) ne 'HASH';
  my $files = $rec->{files};
  return (undef, "no files[]") if ref($files) ne 'ARRAY' || !@$files;

  # Single-file torrent: savepath is simply the directory containing the payload file.
  if (@$files == 1) {
    my $rel = $files->[0]{path}   // '';
    my $len = $files->[0]{length} // 0;
    return (undef, "single-file has no path") unless length $rel;

    my ($leaf) = $rel =~ m{([^/]+)\z};
    return (undef, "single-file leaf missing") unless $leaf;

    Logger::debug("[savepath] single try leaf=[$leaf] t_len=$len rel=[$rel]");

    my $hit = Utils::locate_payload_files_named($leaf, $len);
    if (!$hit) { $hit = Utils::locate_payload_files_named($leaf); }
    return (undef, "single-file no Spotlight hit for [$leaf]") unless $hit && -f $hit;

    my $hsz = (-e $hit) ? (-s $hit) : -1;
    Logger::debug("[savepath] single hit=[$hit] hit_size=$hsz");

    # Size acceptance: exact match OR either side is 0 (reserve-space / recovery zero-byte)
    if (!($len == $hsz || $len == 0 || $hsz == 0)) {
      return (undef, "single-file size mismatch: torrent_len=$len hit_size=$hsz");
    }

    my $savepath = dirname($hit);
    return ($savepath, "single anchor=$leaf hit=$hit");
  }

  # Multi-file: pick anchors and infer savepath by stripping rel suffix; verify under root.
  my @anchors = _pick_anchors($files, 8);
  return (undef, "no usable anchors") if !@anchors;

  for my $a (@anchors) {
    my $rel = $a->{path}   // '';
    my $len = $a->{length} // 0;
    next unless length $rel;

    my ($leaf) = $rel =~ m{([^/]+)\z};
    next unless $leaf;

    Logger::debug("[savepath] multi try leaf=[$leaf] t_len=$len rel=[$rel]");

    my $hit = Utils::locate_payload_files_named($leaf, $len);
    if (!$hit) { $hit = Utils::locate_payload_files_named($leaf); }
    next unless $hit && -f $hit;

    my $hsz = (-e $hit) ? (-s $hit) : -1;
    Logger::debug("[savepath] multi hit=[$hit] hit_size=$hsz");

    # Same size rule: exact OR either side 0
    next unless ($len == $hsz || $len == 0 || $hsz == 0);

    my $savepath = _savepath_from_hit($hit, $rel);
    next unless defined $savepath && length $savepath;

    # Verify a couple files exist under root; allow 0-byte size tolerance too.
    my $ok = _verify_under_root_lenient($savepath, $files, 2);
    return ($savepath, "multi anchor=$leaf hit=$hit") if $ok;
  }

  my @leaf = map { (($_->{path} // '') =~ m{([^/]+)\z}) ? $1 : '' } @anchors;
  @leaf = grep { length } @leaf;
  return (
    undef,
    "no verified payload match via Spotlight (anchors=" .
      join(", ", @leaf[0 .. ($#leaf < 4 ? $#leaf : 4)]) . ")",
  );
}

sub _pick_anchors {
  my ($files, $max) = @_;
  $max ||= 5;

  my %skip_ext = map { $_ => 1 } qw(
    nfo txt jpg jpeg png gif bmp webp heic
    sfv md5 cue log m3u m3u8
    srt sub idx ass ssa
  );

  my @cand = grep { ref($_) eq 'HASH' && defined($_->{path}) } @$files;

  # sort by size descending
  @cand = sort { ($b->{length} // 0) <=> ($a->{length} // 0) } @cand;

  my @out;
  for my $f (@cand) {
    my $p = $f->{path}   // '';
    my $l = $f->{length} // 0;
    next unless length $p;

    # skip tiny stuff
    next if $l && $l < 10_000_000;   # <10MB (tune later)

    my ($leaf) = $p =~ m{([^/]+)\z};
    next unless $leaf;

    my ($ext) = $leaf =~ /\.([^.]+)\z/;
    $ext = lc($ext // '');
    next if $ext && $skip_ext{$ext};

    push @out, { path => $p, length => $l };
    last if @out >= $max;
  }

    # if everything got skipped (small torrents), fall back to largest few anyway
  if (!@out) {
    my $limit = @cand < $max ? scalar(@cand) : $max;
    for (my $i = 0; $i < $limit; $i++) {
      my $f = $cand[$i];
      next unless ref($f) eq 'HASH';
      my $p = $f->{path} // '';
      next unless length $p;
      push @out, { path => $p, length => ($f->{length} // 0) };
    }
  }

  return @out;
}

sub _savepath_from_hit {
  my ($hit, $rel) = @_;

  # rel is like: "TorrentName/dir/file.mkv" or "TorrentName/file.mkv"
  # savepath is the part of $hit before that rel suffix.

  my $suffix = $rel;
  $suffix =~ s{^/+}{};
  $suffix =~ s{/+}{/}g;

  # Normalize hit similarly
  my $h = $hit;
  $h =~ s{/+}{/}g;

  # If hit ends with suffix, strip it.
  if (index($h, $suffix) >= 0 && $h =~ /\Q$suffix\E\z/) {
    my $root = substr($h, 0, length($h) - length($suffix));
    $root =~ s{/$}{};
    return $root if length $root;
  }

  # fallback: if we can’t strip, use dirname(hit) (better than nothing)
  return dirname($hit);
}

sub _verify_under_root {
  my ($root, $files, $need) = @_;
  $need ||= 2;

  my $found = 0;
  for my $f (@$files) {
    next unless ref($f) eq 'HASH';
    my $rel = $f->{path} // '';
    next unless length $rel;

    my $abs = "$root/$rel";
    $abs =~ s{/+}{/}g;

    if (-e $abs) {
      $found++;
      return 1 if $found >= $need;
    }
  }
  return 0;
}

1;
