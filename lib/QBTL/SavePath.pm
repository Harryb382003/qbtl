package QBTL::SavePath;

use common::sense;
use File::Basename qw( dirname basename);

use QBTL::Queue qw ( classify_no_hits );

use Exporter 'import';
our @EXPORT_OK = qw(
    munge_savepath_and_root_rename
    derive_savepath_from_payload
    torrent_top_lvl_from_rec
);

sub derive_savepath_from_payload {
  my ( $app, $ih, %args ) = @_;
  my $rec = $args{rec} || {};
  my $dbg = $args{debug};       # optional arrayref

  return ( undef, "bad rec" ) if ref( $rec ) ne 'HASH';
  my $files = $rec->{files};
  return ( undef, "no files[]" ) if ref( $files ) ne 'ARRAY' || !@$files;

  # 1) pick anchors: largest-first, skip junk
  my @anchors = _pick_anchors( $files, 8 );
  return ( undef, "no usable anchors" ) if !@anchors;

  my $push_dbg = sub {
    return unless $dbg && ref( $dbg ) eq 'ARRAY';
    push @$dbg, @_;
  };

  # 2) try anchors until we get a clean savepath
  for my $a ( @anchors ) {
    my $rel = $a->{path}   // '';
    my $len = $a->{length} // 0;
    next unless length $rel;

    my ( $leaf ) = $rel =~ m{([^/]+)\z};
    next unless $leaf;
    next unless _is_video_path( $leaf );    # never anchor on non-video

    my %attempt = (
                    anchor_rel => $rel,
                    leaf       => $leaf,
                    want_len   => $len,
                    hit_size   => '',
                    hit_any    => '',
                    savepath   => '',
                    verify     => [], );

    # --- Spotlight hit (size-locked first) ---
    my $hit = QBTL::Utils::locate_payload_files_named( $leaf, $len );
    $attempt{hit_size} = $hit // '';

# If size-locked search misses, retry without size (renames/metadata quirks happen)
    unless ( $hit ) {
      $hit = QBTL::Utils::locate_payload_files_named( $leaf );
      $attempt{hit_any} = $hit // '';
    }

    unless ( $hit && -f $hit ) {
      $attempt{reject} = "no_hit";
      $push_dbg->( \%attempt );

      # Preserve instead of vaporize
      QBTL::Queue::classify_no_hits(
                                     $app,
                                     {
                                      ih          => $ih,
                                      name        => ( $attempt{name}   // '' ),
                                      bucket      => ( $attempt{bucket} // '' ),
                                      source_path =>
                                          ( $attempt{source_path} // '' ),
                                      total_size =>
                                          ( $attempt{total_size} // 0 ),
                                      files   => ( $attempt{files}   // [] ),
                                      tracker => ( $attempt{tracker} // '' ),
                                     },
                                     'no_hit: payload not found' );

      next;
    }

    # --- derive savepath by stripping the torrent-relative path ---
    my $savepath = _savepath_from_hit( $hit, $rel );
    $attempt{savepath} = $savepath // '';

    unless ( defined $savepath && length $savepath ) {
      $attempt{reject} = "savepath_from_hit_failed";
      $push_dbg->( \%attempt );
      next;
    }

# --- lightweight verification: check 2 more files exist under computed root ---
    my $ok = _verify_under_root_lenient( $savepath, $files, 2 );
    $attempt{verify} = [ "lenient_ok=" . ( $ok ? 1 : 0 ) ];

    if ( $ok ) {
      $attempt{accept} = 1;
      $push_dbg->( \%attempt );
      return ( $savepath, "anchor=$leaf hit=$hit" );
    }

    $attempt{reject} = "verify_failed";
    $push_dbg->( \%attempt );
    next;
  }

  my @leaf =
      map { ( ( $_->{path} // '' ) =~ m{([^/]+)\z} ) ? $1 : '' } @anchors;
  @leaf = grep {length} @leaf;

  my $anchors_str = @leaf ? join( ",\n  ", @leaf ) : '';

  return ( undef,
         "no verified payload match via Spotlight (anchors=\n  $anchors_str\n)",
  );
}

sub _pick_anchors {
  my ( $files, $max ) = @_;
  $max ||= 5;

  my %skip_ext = map { $_ => 1 } qw(
      nfo txt jpg jpeg png gif bmp webp heic
      sfv md5 cue log m3u m3u8
      srt sub idx ass ssa
  );

  my @cand = grep { ref( $_ ) eq 'HASH' && defined( $_->{path} ) } @$files;

  # sort by size descending
  @cand = sort { ( $b->{length} // 0 ) <=> ( $a->{length} // 0 ) } @cand;

  my @out;
  for my $f ( @cand ) {
    my $p = $f->{path}   // '';
    my $l = $f->{length} // 0;
    next unless length $p;

    # skip tiny stuff
    next if $l && $l < 10_000_000;    # <10MB (tune later)

    my ( $leaf ) = $p =~ m{([^/]+)\z};
    next unless $leaf;

    my ( $ext ) = $leaf =~ /\.([^.]+)\z/;
    $ext = lc( $ext // '' );
    next if $ext && $skip_ext{$ext};

    push @out, {path => $p, length => $l};
    last if @out >= $max;
  }

  # if everything got skipped (small torrents), fall back to largest few anyway
  if ( !@out ) {
    my $limit = @cand < $max ? scalar( @cand ) : $max;
    for ( my $i = 0 ; $i < $limit ; $i++ ) {
      my $f = $cand[$i];
      next unless ref( $f ) eq 'HASH';
      my $p = $f->{path} // '';
      next unless length $p;
      push @out, {path => $p, length => ( $f->{length} // 0 )};
    }
  }

  return @out;
}

sub _is_video_path {
  my ( $path ) = @_;
  return 0 unless defined $path && length $path;

  # strip any query fragments just in case (rare, but harmless)
  $path =~ s/[?#].*\z//;

  # allowlist extensions (add/remove as you like)
  return $path =~ /\.(?:mp4|mkv|avi|mov|wmv|m4v|mpg|mpeg|ts|m2ts|webm|flv)\z/i
      ? 1
      : 0;
}

sub _savepath_from_hit {
  my ( $hit, $rel ) = @_;

  # rel is like: "TorrentName/dir/file.mkv" or "TorrentName/file.mkv"
  # savepath is the part of $hit before that rel suffix.

  my $suffix = $rel;
  $suffix =~ s{^/+}{};
  $suffix =~ s{/+}{/}g;

  # Normalize hit similarly
  my $h = $hit;
  $h =~ s{/+}{/}g;

  # If hit ends with suffix, strip it.
  if ( index( $h, $suffix ) >= 0 && $h =~ /\Q$suffix\E\z/ ) {
    my $root = substr( $h, 0, length( $h ) - length( $suffix ) );
    $root =~ s{/$}{};
    return $root if length $root;
  }

  # fallback: if we can’t strip, use dirname(hit) (better than nothing)
  return dirname( $hit );
}

sub _verify_under_root {
  my ( $root, $files, $need ) = @_;
  $need ||= 2;

  return ( 0, [] ) unless defined( $root )         && length( $root );
  return ( 0, [] ) unless ref( $files ) eq 'ARRAY' && @$files;

  my @rows;
  my $checked = 0;

  for my $f ( @$files ) {
    next unless ref( $f ) eq 'HASH';
    my $rel = $f->{path} // '';
    next unless length $rel;

    my $full   = "$root/$rel";
    my $exists = ( -e $full ) ? 1 : 0;

    push @rows, {rel => $rel, full => $full, exists => $exists};

    $checked++;
    last if $checked >= $need;
  }

  my $ok = 1;
  for my $r ( @rows ) {
    $ok = 0 unless $r->{exists};
  }

  return ( $ok, \@rows );
}

sub _verify_under_root_lenient {
  my ( $root, $files, $need ) = @_;
  $need ||= 2;

  my @rows;

  return ( 0, ["bad root"] )    unless defined $root            && length $root;
  return ( 0, ["bad files[]"] ) unless ref( $files ) eq 'ARRAY' && @$files;

  my $ok = 0;

  for my $f ( @$files ) {
    next unless ref( $f ) eq 'HASH';

    my $rel = $f->{path}   // '';
    my $len = $f->{length} // 0;
    next unless length $rel;

    my $full = "$root/$rel";

    if ( !-e $full ) {
      push @rows, "MISS: $rel";
      next;
    }

    if ( $len > 0 ) {
      if ( !-f $full ) {
        push @rows, "MISS(not file): $rel";
        next;
      }
      my $sz = -s $full;
      if ( !defined $sz || $sz != $len ) {
        push @rows, "MISS(size $sz != $len): $rel";
        next;
      }
    }

    push @rows, "OK: $rel";
    $ok++;
    last if $ok >= $need;
  }

  return ( ( $ok >= $need ) ? 1 : 0, \@rows );
}

sub munge_savepath_and_root_rename {
  my ( %args ) = @_;
  my $rec      = $args{rec};
  my $hit_path = $args{hit_path} // '';
  my $savepath = $args{savepath} // '';

  return ( $savepath, undef ) unless ref( $rec ) eq 'HASH';
  return ( $savepath, undef ) unless length $hit_path;

  my $files = $rec->{files};
  $files = [] unless ref( $files ) eq 'ARRAY';

# "multi" if:
#  - more than one file, OR
#  - any file path contains a slash (root-dir style), which implies a directory context
  my $is_multi = 0;
  if ( @$files > 1 ) {
    $is_multi = 1;
  }
  else {
    for my $f ( @$files ) {
      next unless ref( $f ) eq 'HASH';
      my $p = $f->{path} // '';
      if ( $p =~ m{/} ) {
        $is_multi = 1;
        last;
      }
    }
  }

  # directory containing the hit file on disk
  my $disk_file_dir   = dirname( $hit_path );          # .../Mei Satsuki
  my $disk_root_name  = basename( $disk_file_dir );    # Mei Satsuki
  my $disk_parent_dir = dirname( $disk_file_dir );     # .../M
  my $torrent_top_lvl =
      torrent_top_lvl_from_rec( $rec );    # "Mei Satsuki minipack" ideally
  my $pending_root_rename_data;

  if ( $is_multi ) {

# For multi-file torrents: savepath must be the parent so qbt will create the root folder itself.
    if ( length $disk_parent_dir ) {
      $savepath = $disk_parent_dir;
    }

# If torrent's top folder name differs from disk folder name, queue a root rename.
    if (    length( $torrent_top_lvl )
         && length( $disk_root_name )
         && $torrent_top_lvl ne $disk_root_name )
    {
      $pending_root_rename_data = {
                                   torrent_top_lvl    => $torrent_top_lvl,
                                   drivespace_top_lvl => $disk_root_name,};
    }
  }
  else {
    # Single-file torrent: savepath should be the directory containing the file.
    if ( length $disk_file_dir ) {
      $savepath = $disk_file_dir;
    }
  }

  #   warn(   "\t\t\t\t\t    "
  #         . basename( __FILE__ ) . ":"
  #         . __LINE__
  #         . " MUNGE: is_multi: $is_multi savepath_out: $savepath" );
  return ( $savepath, $pending_root_rename_data );
}

sub torrent_top_lvl_from_rec {
  my ( $rec ) = @_;
  return '' unless ref( $rec ) eq 'HASH';
  my $files = $rec->{files};
  return '' unless ref( $files ) eq 'ARRAY' && @$files;

  my $p = $files->[0]{path} // '';
  return '' unless length $p;

  # top-level is the first path segment
  my ( $top ) = split m{/}, $p, 2;
  return $top // '';
}

1;
