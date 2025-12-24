
package TorrentParser;

use common::sense;
use Data::Dumper;

use Encode         qw(decode);
use File::Copy     qw(move);
use File::Slurp    qw(read_file write_file);
use File::Path     qw(make_path);
use Digest::SHA    qw(sha1_hex);
use File::Basename qw(basename dirname);
use List::Util     qw(sum);
use Bencode        qw(bdecode bencode);
use JSON;
use Exporter 'import';

use lib 'lib/';
use Logger;
use Utils qw(
  start_timer
  stop_timer
);

our @EXPORT_OK = qw(
  torrent_paths
  extract_metadata
  match_by_date
  process_all_infohashes
  normalize_filename
  report_collision_groups
  import_from_parsed

);

my $CACHE_FILE       = "cache/hash_cache.json";
my $problem_log_file = "logs/problem_torrents.json";
my $colliders        = {};    # tracks filename collisions across all torrents

my %hash_cache;

if (-e $CACHE_FILE)
{
  my $json = read_file($CACHE_FILE);
  %hash_cache = %{decode_json($json)};
}

# --- PUBLIC ---

sub new {
  my ($class, $args) = @_;
  my $self = {all_torrents => $args->{all_torrents}, opts => $args->{opts},};
  bless $self, $class;

  unless (ref $self->{all_torrents} eq 'ARRAY')
  {
    die "TorrentParser->new: expected all_torrents => ARRAYREF\n";
  }
  return $self;
}

sub torrent_paths {
  my ($opts) = @_;
  $opts ||= {};

  # Build prune roots from your existing ignore set (export_dir_fin)
  my @prune;

  if (ref($opts->{export_dir_fin}) eq 'ARRAY')
  {
    push @prune, @{$opts->{export_dir_fin}};
  }
  elsif (defined $opts->{export_dir_fin} && length $opts->{export_dir_fin})
  {
    push @prune, $opts->{export_dir_fin};
  }

  if (ref($opts->{exclude}) eq 'ARRAY')
  {
    push @prune, @{$opts->{exclude}};
  }
  elsif (defined $opts->{exclude} && length $opts->{exclude})
  {
    push @prune, $opts->{exclude};
  }

  my $paths =
      locate_items({ext => 'torrent', kind => 'file', prune => \@prune,});

  return wantarray ? @$paths : $paths;
}

sub locate_items {
  Logger::info("Torrent paths locator complete, passing to TorrentParser");
  my ($args) = @_;
  $args ||= {};

  Logger::debug("#\tlocate_items");
  start_timer("locate_items");

  # Check Spotlight availability
  my $mdfind_path = `command -v mdfind 2>/dev/null`;
  chomp $mdfind_path;
  my $has_mdfind = ($mdfind_path && -x $mdfind_path) ? 1 : 0;

  unless ($has_mdfind)
  {
    # no code has been written to use File::Find or any other backend
    # this is a placeholder for future Linux/BSD compatibility
    Logger::error(
       "[Utils] mdfind not available; File::Find fallback not yet implemented");
    stop_timer("locate_items");
    return wantarray ? () : [];
  }

  my $kind  = lc($args->{kind} // 'any');
  my $name  = $args->{name};
  my $ext   = $args->{ext};
  my $limit = $args->{limit};
  my @prune = @{$args->{prune} // []};

  # Build command(s) with proper quoting.
  my @cmds;

  if (defined $name && length $name)
  {
    my $qname = _sh_single_quote($name);
    push @cmds, "mdfind -name $qname 2>/dev/null";

    # Try a dash-normalized variant too (only if it differs)
    my $name2 = $name;

    # Replace common Unicode hyphens/dashes/minus with ASCII '-'
    $name2 =~ s/[\x{2010}\x{2011}\x{2012}\x{2013}\x{2014}\x{2212}]/-/g;

    if (defined $name2 && length($name2) && $name2 ne $name)
    {
      my $qname2 = _sh_single_quote($name2);
      push @cmds, "mdfind -name $qname2 2>/dev/null";
    }
  }
  elsif (defined $ext && length $ext)
  {
    my $query = qq{kMDItemFSName == "*.$ext"cd};
    my $q     = _sh_single_quote($query);
    push @cmds, "mdfind $q 2>/dev/null";
  }
  else
  {
    # legacy default: find *.torrent
    my $query = q{kMDItemFSName == "*.torrent"cd};
    my $q     = _sh_single_quote($query);
    push @cmds, "mdfind $q 2>/dev/null";
  }

  my @out;
  for my $cmd (@cmds)
  {
    my @r = `$cmd`;
    chomp @r;
    @r = map { decode('UTF-8', $_, 1) } @r;    # 1 = FB_DEFAULT (don’t die)
    push @out, @r;
  }

  # filter: existing, dedup, kind, prune, limit
  my %seen;
  my @results = grep { !$seen{$_}++ } grep { defined $_ && length $_ } @out;

  # existence (allow symlinks)
  @results = grep { -e $_ || -l $_ } @results;

  # kind filter
  if ($kind eq 'file')
  {
    @results = grep { -f $_ } @results;
  }
  elsif ($kind eq 'dir')
  {
    @results = grep { -d $_ } @results;
  }

  # prune prefixes
  if (@prune)
  {
    @results = grep {
      my $p  = $_;
      my $ok = 1;
      for my $bad (@prune)
      {
        next unless defined $bad && length $bad;
        my $p_n   = NFD($p);
        my $bad_n = NFD($bad);
        if (index($p, $bad) == 0) { $ok = 0; last }
      }
      $ok
    } @results;
  }

  # limit if requested
  if (defined $limit && $limit =~ /^\d+$/ && $limit > 0 && @results > $limit)
  {
    @results = @results[0 .. $limit - 1];
  }

  Logger::info("[MAIN] Located " . scalar(@results) . " item(s) via Spotlight");
  stop_timer("locate_items");

  Logger::summary("Torrent files located\t\t" . scalar(@results));
  return wantarray ? @results : \@results;
}

# ==========================
# Bucket processing
# ==========================

sub process_all_buckets {
  my ($parsed, $opts) = @_;

  # Only buckets, skip by_infohash
  my $buckets = $parsed->{by_bucket};

  foreach my $bucket (keys %$buckets)
  {
    my $bucket_data = $buckets->{$bucket};
    my $count       = scalar keys %$bucket_data;

    Logger::info("\n[BUCKET] Processing '$bucket' with $count torrents");

    _process_bucket($bucket, $bucket_data, $opts);
  }
}

sub _process_bucket {
  my ($bucket, $bucket_data, $opts) = @_;

  # dev_mode controls chunking
  my $dev_mode = $opts->{dev_mode};
  my $chunk    = $dev_mode ? 5 : scalar keys %$bucket_data;

  my @files = keys %$bucket_data;
  my $count = 0;

  while (@files)
  {
    my @batch = splice(@files, 0, $chunk);

    foreach my $file_path (@batch)
    {
      Logger::info(
                __LINE__ . " [BUCKET:$bucket] Would load into API: $file_path");

      # TODO: replace this stub with actual add_torrent call later
    }

    Logger::info(
           __LINE__ . "[ BUCKET:$bucket] Completed batch of " . scalar(@batch));
    last if $dev_mode;    # stop after first batch if dev_mode enabled
  }
}

sub exclude {
  my ($opts, @new) = @_;
  $opts->{excluded} = normalize_to_arrayref($opts->{excluded});

  # only add defined, non-empty paths
  for my $p (@new)
  {
    next unless defined $p && length $p;
    push @{$opts->{excluded}}, $p;
  }

  return $opts->{excluded};    # return arrayref for convenience
}

sub extract_metadata {
  Logger::debug("# --- EXTRACTING METADATA --- #");
  my ($self, $qbt) = @_;
  my $opts  = $self->{opts};
  my @files = @{$self->{all_torrents}};

  $qbt = {} if ref($qbt) ne 'HASH';

  my %seen = map { $_ => 1 } keys %$qbt;
  my %parsed_by_infohash;
  my %parsed_by_bucket;
  my %bucket_uniques;
  my %bucket_dupes;
  my %bucket_groups;    # grouped collision tracking
  my $intra_dupe_count = 0;
  my $rename_count     = 0;
  my $collision_count  = 0;

  my ($metadata);

  Logger::info("Primed \\%seen with " . scalar(keys %seen) . " qbt infohashes");

  foreach my $file_path (@files)
  {
    Logger::trace("[TorrentParser] Processing $file_path");

    # --- Read torrent ---
    my $raw;
    {
      local $/;
      open my $fh, '<:raw', $file_path or do
          {
            Logger::error("[TorrentParser] Failed to open $file_path: $!");
            next;
          };
      $raw = <$fh>;
      close $fh;
    }

    # --- Decode torrent ---
    my $t = eval { Bencode::bdecode($raw) };
    if ($@ || ref($t) ne 'HASH')
    {
      Logger::warn("[TorrentParser] Failed to bdecode $file_path: $@");
      next;
    }

    my $info = $t->{info};
    if (ref($info) ne 'HASH')
    {
      Logger::warn("[TorrentParser] Missing info dict in $file_path");
      next;
    }

    # --- Compute infohash (v1) from bencoded info dict ---
    my $info_bencoded = eval { Bencode::bencode($info) };
    if ($@ || !defined $info_bencoded)
    {
      Logger::warn(
               "[TorrentParser] Failed to bencode info dict in $file_path: $@");
      next;
    }
    my $infohash = sha1_hex($info_bencoded);

    # --- Name ---
    my $torrent_name = decode_torrent_utf8($info->{name}) // '';

    # --- Normalize file list to [{path, length}] ---
    my @files_norm;

    if (ref($info->{files}) eq 'ARRAY')
    {
      for my $f (@{$info->{files}})
      {
        next unless ref($f) eq 'HASH';

        # Some torrents use path.utf-8
        my $p = exists $f->{'path.utf-8'} ? $f->{'path.utf-8'} : $f->{path};

        my $path_str = '';
        if (ref($p) eq 'ARRAY')
        {
          my @parts = map { defined($_) ? "$_" : '' } @$p;
          @parts    = grep { length } @parts;
          $path_str = join('/', @parts);
        }
        elsif (defined $p)
        {
          $path_str = "$p";
        }

        next unless length $path_str;

        # Prefix root name for multi-file torrents so we can strip later for savepath derivation
        if (length $torrent_name)
        {
          $path_str = "$torrent_name/$path_str"
              unless $path_str =~ m{^\Q$torrent_name\E/};
        }

        push @files_norm, {path => $path_str, length => ($f->{length} // 0)};
      }
    }
    else
    {
      # single-file torrent
      my $len = $info->{length} // 0;
      push @files_norm, {path => $torrent_name, length => $len};
    }

    unless (@files_norm)
    {
      Logger::warn("[TorrentParser] No usable file entries in $file_path");
      next;
    }

    my $total_size = 0;
    $total_size += ($_->{length} || 0) for @files_norm;

    # --- Bucket assignment ---
    my $bucket = _assign_bucket($file_path, $opts);

    # --- Build metadata ---
    $metadata = {infohash    => $infohash,
                 name        => $torrent_name,
                 files       => \@files_norm,
                 total_size  => $total_size,
                 source_path => $file_path,
                 bucket      => $bucket,
                 private     => ($info->{private} ? 'Yes' : 'No'),
                 tracker     => ($t->{announce} // $info->{announce} // ''),
                 comment     => ($t->{comment}  // $info->{comment}  // ''),};

    # --- Optional translation hook ---
    if (my $translation = $opts->{translation})
    {
      if ($metadata->{comment})
      {
        $metadata->{comment} .= "\nEN: $translation";
      }
      else
      {
        $metadata->{comment} = "EN: $translation";
      }
    }

    # --- Dupe / authoritative handling ---
    if (exists $seen{$infohash})
    {
      $intra_dupe_count++;
      $bucket_dupes{$bucket}++;
      next;
    }

    # --- Collision grouping (by bucket) ---
    $bucket_groups{$bucket}{$torrent_name . ".torrent"} //= [];
    push @{$bucket_groups{$bucket}{$torrent_name . ".torrent"}}, $metadata;

    $seen{$infohash}               = 1;
    $parsed_by_infohash{$infohash} = $metadata;
    push @{$parsed_by_bucket{$bucket}}, $metadata;
    $bucket_uniques{$bucket}++;
  }

  Logger::summary(" [BUCKETS]");
  for my $bucket (sort keys %bucket_uniques)
  {
    my $u = $bucket_uniques{$bucket} // 0;
    my $d = $bucket_dupes{$bucket}   // 0;
    my $t = $u + $d;
    Logger::summary(
                    sprintf("   %-20s  total=%6d  uniques=%6d  dupes=%6d",
                            $bucket, $t, $u, $d));
  }

  return {by_infohash => \%parsed_by_infohash,
          by_bucket   => \%parsed_by_bucket,
          uniques     => \%bucket_uniques,
          dupes       => \%bucket_dupes,
          renamed     => $rename_count,
          collisions  => \%bucket_groups,
          intra_dupes => $intra_dupe_count,};
}

sub report_collision_groups {
  my ($bucket_groups) = @_;
  my $collision_groups = 0;

  # Specifically target kitchen_sink (or any bucket you care about)
  if (exists $bucket_groups->{kitchen_sink})
  {
    for my $name (sort keys %{$bucket_groups->{kitchen_sink}})
    {
      my $entries = $bucket_groups->{kitchen_sink}{$name};

      if (@$entries > 1)
      {
        $collision_groups++;

        #                 Logger::info("\n[COLLIDER] $name");
        #                 Logger::info("\t$_->{source_path}") for @$entries;
      }
    }
  }

  Logger::summary(" Filename collision groups observed:\t$collision_groups");
}

sub decode_torrent_utf8 {
  my ($s) = @_;
  return '' unless defined $s;

  # If we already have a Perl utf8 string, keep it
  return $s if utf8::is_utf8($s);

  # Treat as UTF-8 bytes from bencode
  my $out = eval { decode('UTF-8', $s, 1) };    # 1 = FB_CROAK on invalid
  return defined($out) ? $out : $s;             # fallback: raw bytes
}

# sub normalize_filename {
#   my ($l_parsed, $opts, $stats) = @_;
#
#   $stats                    //= {};
#   $stats->{normalized}      //= 0;
#   $stats->{normalized_coll} //= 0;
#   $stats->{normalize_dry}   //= 0;
#
#   my $mode    = $opts->{normalize_mode} // 0;    # 'a' = all, 'c' = colliders
#   my $dry_run = $opts->{dry_run}        // 0;
#
#   if ($mode eq 'c')
#   {
#     my $colliders = $l_parsed->{collisions}{kitchen_sink} // {};
#     for my $name (sort keys %$colliders)
#     {
#       my $entries = $colliders->{$name};
#       next unless @$entries > 1;
#       _normalize_single($_, $opts, $stats, 1) for @$entries;
#     }
#   }
#   elsif ($mode eq 'a')
#   {
#     my $all_meta = $l_parsed->{by_bucket}{kitchen_sink} // [];
#     for my $meta (values @$all_meta)
#     {
#       _normalize_single($meta, $opts, $stats, 0);
#     }
#   }
#   else
#   {
#     Logger::info("[normalize_filename] Normalization disabled (mode=0)");
#   }
#
#   # --- Always produce a clean summary ---
#   Logger::summary("\n--- Normalization Summary ---");
#   if ( $stats->{normalized}
#     || $stats->{normalized_coll}
#     || $stats->{normalize_dry})
#   {
#     Logger::summary("    Normalized (all):       $stats->{normalized}")
#         if $stats->{normalized};
#     Logger::summary("    Normalized (colliders): $stats->{normalized_coll}")
#         if $stats->{normalized_coll};
#     Logger::summary("    Dry-run count:          $stats->{normalize_dry}")
#         if $stats->{normalize_dry};
#   }
#   else
#   {
#     Logger::summary("    No normalization changes performed");
#   }
#
#   return;
# }

sub process_all_infohashes {
  Logger::debug("# --- PROCESSING ALL INFOHASHES --- #");
  my ($parsed, $opts) = @_;
  Logger::info("\n[MAIN] Starting process_all_infohashes()");

  Logger::info("Parsed keys: " . join(", ", keys %$parsed));
  my $infohashes = $parsed->{by_infohash};
  my $count      = scalar keys %$infohashes;
  Logger::info("[MAIN] Found $count unique torrents to process");

  foreach my $infohash (sort keys %$infohashes)
  {
    my $meta = $infohashes->{$infohash};

    #         Logger::info(__LINE__ . " [INFOHASH] Processing $infohash "
    #             . "($meta->{bucket}), $meta->{name}");

    # --- Step 1: Verify source .torrent file still exists ---
    unless (-f $meta->{source_path})
    {
      Logger::warn(__LINE__
            . "[INFOHASH:$infohash] Missing source file: $meta->{source_path}");
      next;
    }

    # --- Step 2: Payload sanity checks (Utils::sanity_check_payload) ---
    my $payload_ok = Utils::sanity_check_payload($meta);
    unless ($payload_ok)
    {
      Logger::warn(__LINE__
                . "[INFOHASH:$infohash] Payload sanity check failed, skipping");
      next;
    }

    #         # --- Step 3: Decide save_path ---
    #         my $save_path = Utils::determine_save_path($meta, $opts);
    #         Logger::debug("[INFOHASH:$infohash] save_path = "
    #             . (defined $save_path ? $save_path : "(qBittorrent default)"));
    #
    #         # --- Step 4: Call qBittorrent API (stub for now) ---
    #         Logger::info("[INFOHASH:$infohash] Would call QBT add_torrent: "
    #             . "source=$meta->{source_path}, save_path="
    #             . (defined $save_path ? $save_path : "QBT-default")
    #             . ", category=" . ($meta->{bucket} // "(none)"));
    #
    #         # TODO: $qbt->add_torrent($meta->{source_path}, $save_path, $meta->{bucket});
  }

  Logger::info("[MAIN] Finished process_all_infohashes()");
}

# sub q_add_queue {
#   Logger::debug("BUILDING QUEUE FOR ADDING TORRENTS");
#   my ($l_parsed, $opts) = @_;
#   my @queue = $l_parsed->{by_infohash};
# }

sub _collision_handler {
  my ($meta, $opts, $colliders) = @_;
  return if !$opts->{normalize};    # skip silently if switch is off

  my $bucket = $meta->{bucket};
  my $path   = $meta->{source_path};
  my $base   = basename($path);

  # init counters once
  $colliders->{case_study} //= {authoritative_safe      => 0,
                                kitchen_sink_safe       => 0,
                                kitchen_sink_normalized => 0,
                                kitchen_sink_manual     => 0,
                                kitchen_sink_missing    => 0,};

  # --- Authoritative buckets (qBittorrent managed) ---
  if ($bucket ne 'kitchen_sink')
  {
    Logger::summary(
                   "[NORMALIZE] Authoritative bucket ($bucket) -> safe: $base");
    $colliders->{case_study}{authoritative_safe}++;
    return "authoritative_safe";
  }

  # --- Kitchen sink collisions ---
  if (-e $path)
  {
    my $normalized = Utils::normalize_filename($meta, $colliders);

    if ($normalized ne $path)
    {
      if (-e $normalized)
      {
        Logger::summary(
          "[NORMALIZE] KS collision escalation failed (target exists) -> manual: $normalized"
        );
        push @{$colliders->{manual}}, $path;
        $colliders->{case_study}{kitchen_sink_manual}++;
        return "kitchen_sink_manual";
      }
      Logger::summary(
                   "[NORMALIZE] KS collision normalized: $base -> $normalized");
      $colliders->{case_study}{kitchen_sink_normalized}++;
      return "kitchen_sink_normalized";
    }
    else
    {
      Logger::summary("[NORMALIZE] KS no collision, safe: $base");
      $colliders->{case_study}{kitchen_sink_safe}++;
      return "kitchen_sink_safe";
    }
  }

  Logger::summary("[NORMALIZE] KS missing source file -> ignored: $base");
  $colliders->{case_study}{kitchen_sink_missing}++;
  return "kitchen_sink_missing";
}

# ---------------------------
#       internal helpers
# ---------------------------

sub _assign_bucket {
  my ($path, $opts) = @_;

  # Check against each configured export_dir_fin
  for my $cdir (@{$opts->{export_dir_fin}})
  {
    if (index($path, $cdir) != -1)
    {
      return basename($cdir);    # dynamic bucket name
    }
  }

  # Check against each configured export_dir
  for my $ddir (@{$opts->{export_dir}})
  {
    if (index($path, $ddir) != -1)
    {
      return basename($ddir);    # dynamic bucket name
    }
  }

  # BT_backup (hardcoded, qBittorrent internal)
  return 'BT_backup' if $path =~ /BT_backup/;

  # Default fallback
  return 'kitchen_sink';
}

sub _sh_single_quote {
  my ($s) = @_;
  $s //= '';
  $s =~ s/'/'"'"'/g;
  return "'$s'";
}

sub _normalize_single {
  my ($meta, $opts, $stats, $is_coll) = @_;

  my $old_path     = $meta->{source_path};
  my $torrent_name = $meta->{name};

  # Prevent double renames
  return if $meta->{_normalized};

  # --- Always make a safe filename ---
  my $safe_name = $torrent_name;
  $safe_name .= ".torrent" unless $safe_name =~ /\.torrent$/i;

  my $dir = dirname($old_path);
  my $new_path;

  if ($is_coll)
  {
    # Collision -> tracker/comment prefix
    my $tracker  = $meta->{tracker}  // '';
    my $comment  = $meta->{comment}  // '';
    my $infohash = $meta->{infohash} // '';
    my $prefixed = _prepend_tracker($tracker, $comment, $infohash, $safe_name);

    $new_path = "$dir/$prefixed";
  }
  else
  {
    # Normal singleton
    $new_path = "$dir/$safe_name";
  }

  # --- Nothing to do if same ---
  return if $old_path eq $new_path;

  my $dry_run = $opts->{dry_run};

  if ($dry_run)
  {
    Logger::info(
      "[normalize_filename] Would normalize: \n\t$old_path -> \n\t$new_path (dry-run)"
    );
    $stats->{normalize_dry}++;
    return;
  }

  # --- Try the actual move ---
  if (move($old_path, $new_path))
  {
    $meta->{source_path} = $new_path;
    $meta->{_normalized} = 1;

    if ($is_coll)
    {
      $stats->{normalized_coll}++;
      Logger::info(
        "[normalize_filename] COLLIDER normalized: \n\t$old_path -> \n\t$new_path"
      );
    }
    else
    {
      $stats->{normalized}++;
      Logger::info(
             "[normalize_filename] Normalized: \n\t$old_path -> \n\t$new_path");
    }
  }
  else
  {
    Logger::warn(
      "[normalize_filename] Failed to normalize:\n\t$old_path ->\n\t$new_path: $!"
    );
  }
}

# sub _prepend_tracker {
#   my ($tracker, $comment, $infohash, $filename) = @_;
#
#   my $prefix = '';
#
#   if ($tracker)
#   {
#     $prefix = _shorten_tracker($tracker);
#   }
#   elsif ($comment && $comment =~ m{https?://([^/]+)})
#   {
#     $prefix = _shorten_tracker($1);
#   }
#
#   if ($comment && $comment =~ m{torrents\.php\?id=(\d+)})
#   {
#     $prefix .= "][$1";
#   }
#
#   if (!$prefix && $infohash)
#   {
#     my $short_hash = substr($infohash, 0, 6);
#     $prefix = $short_hash;
#   }
#
#   # Nothing usable -> just return the original filename
#   unless ($prefix) { return $filename }
#
#   return "[$prefix] $filename";
# }

# sub _shorten_tracker {
#   my ($url) = @_;
#   return '' unless $url;
#
#   # Extract hostname
#   my $host;
#   if ($url =~ m{https?://([^/:]+)})
#   {
#     $host = $1;
#   }
#   else
#   {
#     $host = $url;    # fallback
#   }
#
#   # Strip "tracker." and "www."
#   $host =~ s/^(tracker|www)\.//i;
#
#   # Take only first label before dot
#   my @parts = split(/\./, $host);
#   my $short = $parts[0] // $host;
#
#   return uc $short;    # always uppercase for consistency
# }

# sub _collision_handler {
#   my ($meta, $opts, $colliders) = @_;
#   return if !$opts->{normalize};    # skip silently if switch is off
#
#   my $bucket = $meta->{bucket};
#   my $path   = $meta->{source_path};
#   my $base   = basename($path);
#
#   # init counters once
#   $colliders->{case_study} //= {authoritative_safe      => 0,
#                                 kitchen_sink_safe       => 0,
#                                 kitchen_sink_normalized => 0,
#                                 kitchen_sink_manual     => 0,
#                                 kitchen_sink_missing    => 0,};
#
#   # --- Authoritative buckets (qBittorrent managed) ---
#   if ($bucket ne 'kitchen_sink')
#   {
#     Logger::summary(
#                    "[NORMALIZE] Authoritative bucket ($bucket) -> safe: $base");
#     $colliders->{case_study}{authoritative_safe}++;
#     return "authoritative_safe";
#   }
#
#   # --- Kitchen sink collisions ---
#   if (-e $path)
#   {
#     my $normalized = Utils::normalize_filename($meta, $colliders);
#
#     if ($normalized ne $path)
#     {
#       if (-e $normalized)
#       {
#         Logger::summary(
#           "[NORMALIZE] KS collision escalation failed (target exists) -> manual: $normalized"
#         );
#         push @{$colliders->{manual}}, $path;
#         $colliders->{case_study}{kitchen_sink_manual}++;
#         return "kitchen_sink_manual";
#       }
#       Logger::summary(
#                    "[NORMALIZE] KS collision normalized: $base -> $normalized");
#       $colliders->{case_study}{kitchen_sink_normalized}++;
#       return "kitchen_sink_normalized";
#     }
#     else
#     {
#       Logger::summary("[NORMALIZE] KS no collision, safe: $base");
#       $colliders->{case_study}{kitchen_sink_safe}++;
#       return "kitchen_sink_safe";
#     }
#   }
#
#   Logger::summary("[NORMALIZE] KS missing source file -> ignored: $base");
#   $colliders->{case_study}{kitchen_sink_missing}++;
#   return "kitchen_sink_missing";
# }

1;
