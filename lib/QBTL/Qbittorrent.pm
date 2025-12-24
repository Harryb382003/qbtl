package QBittorrent;

# ----------------------------------------------------------------------
#       This module for for addressing qBittorrent'a API functions
# ----------------------------------------------------------------------

use common::sense;
use Data::Dumper;
use File::Basename;
use File::Slurp;
use JSON;
use Time::Piece;
use File::Spec;
use LWP::UserAgent;
use HTTP::Cookies;
use HTTP::Request::Common qw(POST);


use lib 'lib/';
use Logger;
use Utils qw(
    start_timer
    stop_timer
    );


# sub new {
# 	Logger::debug("#	new");
#     my ($class, $opts) = @_;
#     my $self = {
#         base_url => $opts->{base_url} || 'http://localhost:8080',
#         username => $opts->{username} || 'admin',
#         password => $opts->{password} || 'adminadmin',
#         ua       => LWP::UserAgent->new(cookie_jar => HTTP::Cookies->new),
#         %$opts,
#     };
#     bless $self, $class;
#     $self->_login;
#     return $self;
# }
#
# sub _login {
# 	Logger::debug("#	_login");
#     my $self = shift;
#     my $res = $self->{ua}->post(
#         "$self->{base_url}/api/v2/auth/login",
#         {
#             username => $self->{username},
#             password => $self->{password}
#         }
#     );
#
#     die "Login failed: " . $res->status_line unless $res->is_success;
# }
#
# sub get_preferences {
# 	Logger::debug("#	get_preferences");
#     my $self = shift;
#     my $res = $self->{ua}->get("$self->{base_url}/api/v2/app/preferences");
#
#     die "Failed to get preferences: " . $res->status_line unless $res->is_success;
#
#     return decode_json($res->decoded_content);
# }
#
# sub get_torrents_infohash {
# 	Logger::debug("#	get_torrents");
#     start_timer("qBittorrent connect");
#     my $self = shift;
#     my $res = $self->{ua}->get("$self->{base_url}/api/v2/torrents/info");
#
#     die "Failed to get torrents: " . $res->status_line unless $res->is_success;
#
#     my $torrents = decode_json($res->decoded_content);
#     my %hash = map { $_->{hash} => $_ } @$torrents;
#     Logger::summary("Torrents loaded in qBittorrent     \t" . scalar(keys %hash));
# #    Logger::info("Successfully loaded into cache " . scalar(keys %hash));
#     stop_timer("qBittorrent connect");
#     return \%hash;
# }
#
# sub import_from_parsed {
#     Logger::debug("# --- IMPORTING PARSED METADATA --- #");
#     my ($self, $parsed, $opts) = @_;
#
#     my $by_infohash = $parsed->{by_infohash} || {};
#     my @pending;
#
#     for my $infohash (sort keys %$by_infohash) {
#         my $metadata = $by_infohash->{$infohash} || next;
#
#         # require: source_path, files arrayref, and at least one entry with a path
#         next unless $metadata->{source_path}
#                  && ref($metadata->{files}) eq 'ARRAY'
#                  && @{ $metadata->{files} }
#                  && defined($metadata->{files}[0]{path})
#                  && length($metadata->{files}[0]{path});
#
#         push @pending, {
#             infohash      => $infohash,
#             name          => ($metadata->{name} // '(unnamed)'),
#             source_path   => $metadata->{source_path},     # absolute .torrent path
#             files         => $metadata->{files},           # [{ path, length }, ...]
#             bucket        => $metadata->{bucket},
#             resolved_path => $metadata->{resolved_path},   # may be undef
#         };
#     }
#
#     return wantarray ? @pending : \@pending;
# }
#
# sub get_torrents_info {
#     Logger::debug("#	get_torrents_info");
#     my $self = shift;
#
#     my $res = $self->{ua}->get("$self->{base_url}/api/v2/torrents/info");
#     die "Failed to get torrents: " . $res->status_line unless $res->is_success;
#
#     my $torrents = decode_json($res->decoded_content);
#     return $torrents;   # arrayref of hashrefs
# }
#
# sub torrent_exists {
#     my ($self, $hash) = @_;
#
#     my $res = $self->{ua}->get("$self->{base_url}/api/v2/torrents/info?hashes=$hash");
#     die "Failed to check torrent: " . $res->status_line unless $res->is_success;
#
#     my $arr = decode_json($res->decoded_content);
#     return (ref($arr) eq 'ARRAY' && @$arr) ? 1 : 0;
# }
#
# sub add_torrent_file {
#   my ($self, $torrent_path, $savepath) = @_;
#
#   require HTTP::Request::Common;
#
#   my @content = (torrents => [$torrent_path]);
#
#   # THIS is the key: qBittorrent expects the field name "savepath"
#   if (defined $savepath && length $savepath) {
#     push @content, (savepath => $savepath);
#   }
#
#   my $req = HTTP::Request::Common::POST(
#     "$self->{base_url}/api/v2/torrents/add",
#     Content_Type => 'form-data',
#     Content      => \@content,
#   );
#
#   my $res = $self->{ua}->request($req);
#
#   return {
#     ok   => ($res->is_success ? 1 : 0),
#     code => ($res->code // 0),
#     body => ($res->decoded_content // ''),
#   };
# }
#
# sub recheck_hash {
#   my ($self, $hash) = @_;
#   die "bad hash" unless defined $hash && $hash =~ /^[0-9a-f]{40}$/i;
#
#   require HTTP::Request::Common;
#
#   my $req = HTTP::Request::Common::POST(
#     "$self->{base_url}/api/v2/torrents/recheck",
#     Content_Type => 'application/x-www-form-urlencoded',
#     Content      => [ hashes => $hash ],
#   );
#
#   my $res = $self->{ua}->request($req);
#
#   die "Failed to recheck: " . $res->status_line . " body=" . ($res->decoded_content // '')
#     unless $res->is_success;
#
#   return 1;
# }


# sub add_torrent_file_paused {
#     my ($self, $torrent_path) = @_;
#
#     die "torrent file not found: $torrent_path" unless -e $torrent_path;
#
#     my $req = POST(
#         "$self->{base_url}/api/v2/torrents/add",
#         Content_Type => 'form-data',
#         Content      => [
#             torrents => [$torrent_path],
#             paused   => 'true',
#         ],
#     );
#
#     my $res = $self->{ua}->request($req);
#
#     return {
#         ok   => ($res->is_success ? 1 : 0),
#         code => ($res->code // 0),
#         body => ($res->decoded_content // ''),
#     };
# }

#
# sub add_torrent_file {
#     my ($self, $torrent_path) = @_;
#
#     die "torrent file not found: $torrent_path" unless -e $torrent_path;
#
#     my $res = $self->{ua}->post(
#         "$self->{base_url}/api/v2/torrents/add" => form => {
#             torrents => { file => $torrent_path },
#         }
#     );
#
#     return {
#         ok   => ($res->is_success ? 1 : 0),
#         code => ($res->code // 0),
#         body => ($res->decoded_content // ''),
#     };
# }


# sub recheck_hash {
#     my ($self, $hash) = @_;
#
#     my $url = "$self->{base_url}/api/v2/torrents/recheck?hashes=$hash";
#     my $res = $self->{ua}->get($url);
#
#     if (!$res->is_success) {
#         die "Failed to recheck: " . $res->status_line;
#     }
#
#     return 1;
# }

# sub delete_hash_keep_files {
#     my ($self, $hash) = @_;
#     my $res = $self->{ua}->post("$self->{base_url}/api/v2/torrents/delete" => form => {
#         hashes      => $hash,
#         deleteFiles => 'false',
#     });
#     die "Failed to delete: " . $res->status_line unless $res->is_success;
#     return 1;
# }




# sub pause_hash {
#     my ($self, $hash) = @_;
#     my $res = $self->{ua}->post("$self->{base_url}/api/v2/torrents/pause" => form => { hashes => $hash });
#     die "Failed to pause: " . $res->status_line unless $res->is_success;
#     return 1;
# }

# sub add_torrent_file_paused {
#     my ($self, $torrent_path, %args) = @_;
#     die "torrent file not found: $torrent_path" unless -e $torrent_path;
#     my $res = $self->{ua}->post(
#         "$self->{base_url}/api/v2/torrents/add" => form => {
#             paused  => 'true',
#             torrents => { file => $torrent_path },
#         }
#     );
#     return {
#         ok   => ($res->is_success ? 1 : 0),
#         code => ($res->code // 0),
#         body => ($res->decoded_content // ''),
#     };
# }


#
#     # --- prune runtime queue: never process quarantined torrents ---
#     my $before = scalar @pending;
#     @pending = grep { (($_->{bucket} // '') ne 'temp_ignore') } @pending;
#     my $removed = $before - scalar @pending;
#     if ($removed > 0) {
#         Logger::info(sprintf(
#             "[TEMP] pruned %d quarantined torrent(s) from pending queue", $removed
#         ));
#     };


=pod
#
#     # Chunk pacing: --chunk N[a|m], optional --delay
#     my ($chunk_size, $auto_continue, $manual_prompt) =
#         parse_chunk_spec($opts->{chunk});
#
#     my $torrents_processed = 0;
#
#     # Ensure session (repo currently exposes _login)
#     $self->_login();
#
#     while (@pending) {
#         my $torrent_batch = chunk(\@pending, $chunk_size);   # NOTE:  chunk() is destructive
#
#         for my $entry (@$torrent_batch) {
#             my $payload_check = payload_ok($entry, $opts);
#             unless ($payload_check->{ok}) {
#                 my $miss = $payload_check->{details}{missing} // [];
#                 my $why  = $payload_check->{reason} // 'unknown';
#
#                 Logger::warn(
#                     "[SKIP] $entry->{name} ($entry->{infohash}) "
#                     . "reason=$why "
#                     . "missing_count=" . scalar(@$miss)
#                 );
#
#                 # move the .torrent to temp_ignore so it stops resurfacing
#                 Utils::quarantine_torrent($entry->{source_path}, $opts, $why);
#
#                 next;
#             }
#
#             Logger::warn("[CORRO] zero-byte placeholder(s) found in $entry->{name}")
#                 if $payload_check->{needs_corroboration};
#
#             my $save_path = derive_save_path($entry, $opts);
#
#             if ($opts->{dry} || $opts->{dev_mode}) {
#                 Logger::info("[DRY] add: $entry->{source_path} -> " . ($save_path // '(qbt default)'));
#             } else {
#                 my $ok = $self->add_torrent($entry->{source_path}, $save_path);
#                 if ($ok) {
#                     Logger::summary("[ADDED] $entry->{source_path}");
#                 } else {
#                     Logger::summary("[FAILED] add $entry->{source_path}");
#                 }
#             }
#             $torrents_processed++;
#         }
#
#         # default behavior (no flags): single chunk then exit
#         last if (!$auto_continue && !$manual_prompt);
#
#         # manual prompt between chunks
#         if ($manual_prompt) {
#             last unless prompt_between_chunks(1, $torrents_processed);
#         }
#
#         # auto delay between chunks
#         pause_between_chunks($auto_continue, $opts->{delay});
#     }
#
#     return 1;
}




sub get_torrent_files {
    my ($self, $infohash) = @_;
    my $res = $self->{ua}->get("$self->{base_url}/api/v2/torrents/files?hash=$infohash");

    unless ($res->is_success) {
        Logger::warn("[WARN] Failed to fetch files for torrent $infohash: " . $res->status_line);
        return;
    }

    my $files;
    eval { $files = decode_json($res->decoded_content); 1 }
        or do {
            Logger::warn("[WARN] Failed to parse JSON for torrent $infohash: $@");
            return;
        };

    return $files;
}


sub add_torrent {
    my ($self, $torrent_path, $save_path, $category) = @_;

    unless (defined $torrent_path && -f $torrent_path) {
        Logger::error("[QBT] Torrent file not found: $torrent_path");
        return 0;
    }

    if ($self->{opts}{dev_mode}) {
        Logger::info("[QBT] [DRY] would add: $torrent_path -> " . ($save_path // '(qbt default)'));
        return 1;
    }

    my $ua   = $self->{ua}       or die "UA not initialized";
    my $base = $self->{base_url} or die "Base URL not set";
    my $url  = "$base/api/v2/torrents/add";

    my @content = ( torrents => [$torrent_path], paused => "true" );
    push @content, (savepath => $save_path) if defined $save_path && length $save_path;
    push @content, (category => $category) if defined $category && length $category;

    my $res   = $ua->post($url, Content_Type => 'form-data', Content => \@content);
    my $code  = $res->code;
    my $body  = $res->decoded_content // '';
    $body =~ s/^\s+|\s+$//g;

    # Spec-observed hard fail
    if ($code == 415) {
        Logger::summary("[FAILED] $torrent_path -> " . ($save_path // '(qbt default)') . " reason=invalid msg='415
Torrent file is not valid'");
        return 0;
    }

    # Any other HTTP error (rare, but be safe)
    unless ($code == 200) {
        Logger::summary("[FAILED] $torrent_path -> " . ($save_path // '(qbt default)') . " reason=http-error code=$code
msg='$body'");
        return 0;
    }

    # 200 OK -> inspect body text (plain text responses from qBT)
    if ($body =~ /^(?:ok\.?)$/i) {
        Logger::summary("[ADDED]   $torrent_path -> " . ($save_path // '(qbt default)') . " ($body)");
        return 1;
    }
    if ($body =~ /exist|duplicate/i) {
        Logger::summary("[EXISTED] $torrent_path -> " . ($save_path // '(qbt default)') . " ($body)");
        return 1;  # idempotent success
    }
    if ($body =~ /fail|invalid|unsupported|error/i) {
        Logger::summary("[FAILED]  $torrent_path -> " . ($save_path // '(qbt default)') . " reason=api msg='$body'");
        return 0;
    }

    # Unknown but 200 — treat as success and log what we saw
    Logger::summary("[ADDED?]  $torrent_path -> " . ($save_path // '(qbt default)') . " (body='$body')");
    return 1;
}

sub force_recheck {
    my ($infohash) = @_;
    Logger::info("[QBT] Would force recheck: $infohash");
    return 1;
}

sub get_torrent_info {
    my ($infohash) = @_;
    Logger::info("[QBT] Would fetch torrent info: $infohash");
    # Return a dummy structure
    return {
        infohash   => $infohash,
        progress   => 0.42,   # 42% complete
        size_bytes => 5_000_000_000, # 5 GB
        category   => undef,
        state      => "paused",
    };
}

sub delete_torrent {
    my ($infohash, $with_data) = @_;
    Logger::info("[QBT] Would delete torrent: $infohash (with_data=" . ($with_data ? "YES" : "NO") . ")");
    return 1;
}

sub get_free_space {
    my ($path) = @_;
    Logger::info("[QBT] Would check free space at: $path");
    return 500 * 1024 * 1024 * 1024; # pretend 500 GB free
}


sub get_q_zombies {
	Logger::debug("#	get_q_zombies");
    my ($self, $opts) = @_;

    my %zombies;
    my $cache_dir = "cache";
    my $cache_pattern = "$cache_dir/qbt_zombies_*.json";

    # --- DEV MODE: Use latest cache if present ---
    if ($opts->{dev_mode} && !$opts->{scan_zombies}) {
        my @cache_files = glob($cache_pattern);
        if (@cache_files) {
            my $latest_cache = (sort @cache_files)[-1];
            Logger::info("[CACHE] Loading zombie torrent list from: $latest_cache");
            my $json = read_file($latest_cache);
            my $cached = decode_json($json);
            Logger::summary("[CACHE] Zombie torrents in qBittorrent:\t" . scalar keys %$cached);
            return $cached;
        }
        else {
            Logger::warn("[WARN] No zombie cache found. Run with --scan-zombies to create it.");
            return {};
        }
    }

    # --- PROD MODE: Skip unless scan requested ---
    if (!$opts->{scan_zombies}) {
        Logger::info("[INFO] Skipping zombie scan. Use --scan-zombies to enable.");
        return {};
    }

    # --- LIVE SCAN ---
    Logger::info("[INFO] Scanning qBittorrent for zombie torrents (missing content)...");
    my $session = $self->get_torrents(); # { infohash => {...} }

    foreach my $ih (keys %$session) {
        my $res = $self->{ua}->get("$self->{base_url}/api/v2/torrents/files?hash=$ih");
        next unless $res->is_success;

        my $files = decode_json($res->decoded_content);
        if (!@$files) { # empty list means zombie
            $zombies{$ih} = $session->{$ih};
        }
    }

    Logger::summary("[LIVE] Zombie torrents in qBittorrent:\t" . scalar keys %zombies);

    # --- Update Cache ---
    eval {
        make_path($cache_dir) unless -d $cache_dir;
        my $ts = strftime("%y.%m.%d-%H%M", localtime);
        my $cache_file = "$cache_dir/qbt_zombies_$ts.json";
        write_file($cache_file, JSON->new->utf8->pretty->encode(\%zombies));
        Logger::info("[CACHE] Zombie list updated: $cache_file");
    };
    Logger::warn("[WARN] Failed to update zombie cache: $@") if $@;

    return \%zombies;
}

=cut



1;


