package QBTL::Web;
use common::sense;

use File::Spec;
use FindBin qw($Bin);
use Mojolicious;
use Mojo::JSON qw(true);

use QBTL::LocalCache;
use QBTL::Parse;
use QBTL::Plan;
use QBTL::QBT;
use QBTL::Scan;

use Digest::SHA qw(sha1_hex);

sub _fmt_ts {
  my ($epoch) = @_;
  return '' unless $epoch;
  my @lt = localtime($epoch);
  return sprintf("%04d-%02d-%02d %02d:%02d:%02d",
    $lt[5]+1900, $lt[4]+1, $lt[3], $lt[2], $lt[1], $lt[0]);
}

sub _fmt_savepath_debug {
  my ($dbg) = @_;
  return '' unless $dbg && ref($dbg) eq 'ARRAY' && @$dbg;

  my @out;

  push @out, "";
  push @out, "SavePath debug (most recent attempts):";

  for my $a (@$dbg) {
    next unless ref($a) eq 'HASH';

    push @out, "";
    push @out, "-" x 50;
    push @out, "Anchor:";
    push @out, sprintf("  rel:      %s", $a->{anchor_rel} // '');
    push @out, sprintf("  leaf:     %s", $a->{leaf}       // '');
    push @out, sprintf("  want_len: %s", $a->{want_len}   // '');

    push @out, "";
    push @out, "Spotlight:";
    push @out, "  size-locked hit: " . ($a->{hit_size} || '(none)');
    push @out, "  name-only hit:  " . ($a->{hit_any}  || '(none)');

    push @out, "";
    push @out, "Derived savepath:";
    push @out, "  " . ($a->{savepath} || '(none)');

    push @out, "";
    push @out, "Verification:";

    if ($a->{verify} && ref($a->{verify}) eq 'ARRAY' && @{ $a->{verify} }) {
      for my $v (@{ $a->{verify} }) {
        next unless ref($v) eq 'HASH';
        my $flag = $v->{exists} ? '[OK]  ' : '[MISS]';
        push @out, "  $flag $v->{full}";
      }
    } else {
      push @out, "  (not attempted)";
    }

    push @out, "";
    push @out, "Result:";
    push @out, "  " . ($a->{accept} ? 'ACCEPTED' : "rejected: $a->{reject}");
  }

  push @out, "-" x 50;
  push @out, "";

  return join("\n", @out);
}

sub _tasks_ref {
  my ($app) = @_;
  $app->defaults->{tasks} ||= {};
  return $app->defaults->{tasks};
}

sub _task_counts {
  my ($app) = @_;
  my $tasks = _tasks_ref($app);

  my %counts;
  for my $hash (keys %$tasks) {
    my $t = $tasks->{$hash};
    next unless ref($t) eq 'HASH';
    my $stage = $t->{stage} || 'unknown';
    $counts{$stage}++;
  }

  my $total = scalar(keys %$tasks);
  return ($total, \%counts);
}

sub _task_fail_items {
  my ($app, $limit) = @_;
  $limit ||= 200;

  my $tasks = _tasks_ref($app);
  my @fails;

  for my $hash (keys %$tasks) {
    my $t = $tasks->{$hash};
    next unless ref($t) eq 'HASH';
    next unless ($t->{stage} || '') eq 'fail';
    push @fails, $t;
  }

  # newest first
  @fails = sort { ($b->{ts}||0) <=> ($a->{ts}||0) } @fails;

  if (@fails > $limit) {
    @fails = @fails[0 .. ($limit - 1)];
  }
  return \@fails;
}

sub _qbt_observer_tick {
  my ($app, $opts) = @_;
  $opts ||= {};

  my $tasks = _tasks_ref($app);
  $app->defaults->{observer_last_tick} = time;
  $app->defaults->{observer_last_count} = scalar(keys %$tasks);
  return unless %$tasks;  # nothing to do
  $app->log->debug("[observer] tick: tasks=" . scalar(keys %$tasks));

  my $qbt;
  eval {
    $qbt = QBTL::QBT->new(opts => $opts);
    1;
  } or do {
    my $err = "$@";
    chomp $err;
    # mark global failure on all tasks (light-touch)
    for my $hash (keys %$tasks) {
      my $t = $tasks->{$hash};
      next unless ref($t) eq 'HASH';
      $t->{stage}  = 'fail';
      $t->{reason} = "observer: QBT init failed: $err";
      $t->{ts}     = time;
    }
    return;
  };

  for my $hash (keys %$tasks) {
    next unless $hash =~ /^[0-9a-f]{40}$/;

    my $arr = eval { $qbt->get_torrents_info(hashes => $hash) };
    if ($@) {
      my $err = "$@";
      chomp $err;
      my $t = ($tasks->{$hash} ||= { hash => $hash });
      $t->{stage}  = 'fail';
      $t->{reason} = "observer: get_torrents_info failed: $err";
      $t->{ts}     = time;
      next;
    }

    # missing in qBittorrent == "new"
    if (!ref($arr) || ref($arr) ne 'ARRAY' || !@$arr) {
      my $t = ($tasks->{$hash} ||= { hash => $hash });
      $t->{stage} = 'new';
      $t->{ts}    = time;
      next;
    }

    my $t0 = $arr->[0];
    my $state    = (ref($t0) eq 'HASH') ? ($t0->{state}    // '') : '';
    my $progress = (ref($t0) eq 'HASH') ? ($t0->{progress} // -1) : -1;

    my $t = ($tasks->{$hash} ||= { hash => $hash });
    $t->{last_state}    = $state;
    $t->{last_progress} = $progress;
    $t->{ts}            = time;

    if ($state =~ /^checking/i) {
      $t->{stage} = 'rechecking';
      next;
    }

    # conservative: progress > 0 means “known good location”
    if (defined $progress && $progress > 0) {
      $t->{stage} = 'ready';     # ready-to-resume (action later)
      next;
    }

    # otherwise it’s still suspect
    $t->{stage} = 'suspect_zero';
  }

  return;
}

sub _infohash_from_torrent_file {
  my ($path) = @_;
  # Minimal bencode extract: get the raw "info" dictionary bytes and SHA1 them.
  # If you already have a torrent parser/bencode module in your legacy code, use that instead.
  require Bencode;  # if you have it
  my $raw = do {
    open my $fh, '<:raw', $path or die "open($path): $!";
    local $/;
    <$fh>;
  };
  my $t = Bencode::bdecode($raw);
  die "not a torrent (no info dict)" unless ref($t) eq 'HASH' && ref($t->{info}) eq 'HASH';
  my $info_bencoded = Bencode::bencode($t->{info});
  return sha1_hex($info_bencoded);
}

sub _task_upsert {
  my ($app, $hash, %patch) = @_;
  $hash = lc($hash // '');

  $app->defaults->{tasks} ||= {};
  my $t = ($app->defaults->{tasks}{$hash} ||= { hash => $hash, ts_created => time });

  $t->{ts_updated} = time;
  @$t{keys %patch} = values %patch;

  return $t;
}

sub app {
  my ($opts) = @_;
    $opts ||= {};
  my $app = Mojolicious->new;
    $app->defaults->{dev_mode} = ($opts->{dev_mode} ? 1 : 0);
    $app->defaults->{root_dir} = $opts->{root_dir} || '.';
  my $root = $opts->{root_dir} || '.';
  push @{ $app->renderer->paths }, File::Spec->catdir($root, 'templates');
  push @{ $app->static->paths },   File::Spec->catdir($root, 'ui');

  # Serve static files from /ui
  my $ui_dir = "$Bin/../ui";
  push @{ $app->static->paths }, $ui_dir;

  my $r = $app->routes;

  $r->get('/' => sub {
  my $c = shift;
  $c->stash(dev_mode => ($opts->{dev_mode} ? 1 : 0));

  # ---------- Load local cache ----------
  my $local_by_ih = QBTL::LocalCache::get_local_by_ih(
    root_dir   => ($opts->{root_dir} || '.'),
    opts_local => { torrent_dir => "/" },
  );
  $local_by_ih = {} if ref($local_by_ih) ne 'HASH';

  my $local_count = scalar(keys %$local_by_ih);

  # ---------- Load qBittorrent state ----------
  my ($qbt_by_ih, $qbt_list);
  my $qbt_err = '';
  eval {
    my $qbt = QBTL::QBT->new(opts => {});
    $qbt_by_ih = $qbt->get_torrents_infohash();   # { hash => {...} }
    $qbt_by_ih = {} if ref($qbt_by_ih) ne 'HASH';
    $qbt_list  = $qbt->get_torrents_info() || []; # [ {...}, ... ]
    1;
  } or do {
    $qbt_err = "$@";
    $qbt_by_ih = {};
    $qbt_list  = [];
  };

  my $qbt_loaded_count = scalar(keys %$qbt_by_ih);

  # ---------- Compute qbt name-as-hash + repairable ----------
  my $name_is_hash = 0;
  my $repairable   = 0;

  for my $t (@$qbt_list) {
    next if ref($t) ne 'HASH';
    my $name = $t->{name} // '';
    next unless $name =~ /^[0-9a-fA-F]{40}$/;
    $name_is_hash++;

    my $ih = lc($t->{hash} // '');
    next unless $ih =~ /^[0-9a-f]{40}$/;
    $repairable++ if exists $local_by_ih->{$ih};
  }

  # ---------- Missing-from-qbt (local not loaded) ----------
  my $missing_from_qbt = 0;
  for my $ih (keys %$local_by_ih) {
    $missing_from_qbt++ if !exists $qbt_by_ih->{$ih};
  }

  # ---------- Runtime counts + failures list ----------
  my %stage_counts;
  my @fails;

  for my $ih (keys %$local_by_ih) {
    my $rec = $local_by_ih->{$ih};
    next if ref($rec) ne 'HASH';

    my $rt = $rec->{runtime};
    next if ref($rt) ne 'HASH'; # only runtime-tracked items count toward stages

    my $stage = $rt->{stage} // 'unknown';
    $stage_counts{$stage}++;

    next unless $stage eq 'unadded' || $stage eq 'error';

    push @fails, {
      hash          => $ih,
      name          => ($rec->{name} // ''),
      stage         => $stage,
      reason        => ($rt->{reason} // ''),
      last_state    => ($rt->{last_state} // ''),
      last_progress => (defined $rt->{last_progress} ? $rt->{last_progress} : ''),
      ts            => ($rt->{ts} // 0),
      source_path   => ($rec->{source_path} // ''),
    };
  }

  # newest failures first
  @fails = sort { ($b->{ts}||0) <=> ($a->{ts}||0) } @fails;

  # Tiny helper: format epoch -> local time
  my $fmt_ts = sub {
    my ($ts) = @_;
    return '' unless $ts;
    my @lt = localtime($ts);
    return sprintf("%04d-%02d-%02d %02d:%02d:%02d",
      $lt[5]+1900, $lt[4]+1, $lt[3], $lt[2], $lt[1], $lt[0]);
  };

  my %stats = (
    local_unique          => $local_count,
    qbt_loaded            => $qbt_loaded_count,
    qbt_name_is_hash      => $name_is_hash,
    missing_from_qbt      => $missing_from_qbt,
    repairable            => $repairable,
    qbt_error             => $qbt_err,
  );
my $tick = $app->defaults->{observer_last_tick} || 0;

  $c->stash(
  stats        => \%stats,
  stage_counts => \%stage_counts,
  fails        => \@fails,
  fmt_ts       => $fmt_ts,

  observer_last_tick_h  => _fmt_ts($tick),
  observer_last_count   => ($app->defaults->{observer_last_count} || 0),
);

  $c->render(template => 'index');
});

  $r->get('/opts' => sub {
    my $c = shift;
      $c->render(json => { dev_mode => ($opts->{dev_mode} ? 1 : 0) });
  });

  $r->get('/health' => sub {
    my $c = shift;
      $c->render(json => { ok => true, app => 'qbtl' });
  });

  $r->get('/legacy_smoke' => sub {
    my $c = shift;
    my $out = {};
    eval {
      require Utils;
      $out->{os} = Utils::test_OS();
      1;
    } or do {
      $out->{error} = "$@";
    };
    $c->render(json => $out);
  });

  $r->get('/parse_smoke' => sub {
    my $c = shift;
  # Minimal opts + empty torrent list for smoke test
    my $res = eval { QBTL::Parse::run(all_torrents => [], opts => {}) };
    if ($@) {
      return $c->render(json => { error => "$@" });
    }
    $c->render(json => { ok => Mojo::JSON->true });
  });

  $r->get('/plan_preview' => sub {
  my $c = shift;

  my $opts = { torrent_dir => "/" };  # temporary; we’ll make this configurable next

  my $scan = eval { QBTL::Scan::run(opts => $opts) };
  if ($@) {
    return $c->render(json => { error => "scan: $@" });
  }

  my $qbt_by_ih = eval {
    my $qbt = QBTL::QBT->new(opts => $opts);
    my $h   = $qbt->get_torrents_infohash();   # { infohash => {...} }
    $h = {} if ref($h) ne 'HASH';
    $h;
  };
  if ($@) {
    return $c->render(json => { error => "qbt: $@" });
  }

  # Build local ih => path map using parser
  my $parsed = eval {
    QBTL::Parse::run(
      all_torrents   => $scan->{torrents},
      opts           => $opts,
      qbt_loaded_tor => $qbt_by_ih,     # <-- FIX 1: was $qbt_set
    );
  };
  if ($@) {
    return $c->render(json => { error => "parse: $@" });
  }

  my $local_by_ih = $parsed->{by_infohash} || $parsed->{infohash_map} || {};
  if (ref($local_by_ih) ne 'HASH' || !keys %$local_by_ih) {
    return $c->render(json => {
      error     => "No local infohash map found in parsed data (expected by_infohash/infohash_map).",
      keys_seen => [ grep { defined } keys %$parsed ],
    });
  }

  my $plan = QBTL::Plan::missing_infohashes(
    local_by_ih => $local_by_ih,
    qbt_set     => $qbt_by_ih,          # <-- FIX 2: was $qbt_set
  );

  $c->render(json => {
    local_unique_infohashes => $plan->{local_count},
    qbt_loaded_count        => $plan->{qbt_count},
    overlap_count           => $plan->{overlap},
    missing_count           => scalar(@{ $plan->{missing} }),
    sample_overlap          => undef,
  });
});

  $r->get('/qbt' => sub {
    my $c = shift;
    $c->reply->static('qbt.html');
  });

$r->get('/qbt/add_one' => sub {
  my $c = shift;
  $c->stash(dev_mode => ($opts->{dev_mode} ? 1 : 0));

  my $local_by_ih = QBTL::LocalCache::get_local_by_ih(
    root_dir   => ($opts->{root_dir} || '.'),
    opts_local => { torrent_dir => "/" },
  );
  $local_by_ih = {} if ref($local_by_ih) ne 'HASH';

  my $qbt = QBTL::QBT->new(opts => {});
  my $qbt_by_ih = $qbt->get_torrents_infohash;
  $qbt_by_ih = {} if ref($qbt_by_ih) ne 'HASH';

  my ($pick_hash) = grep { !exists $qbt_by_ih->{$_} } keys %$local_by_ih;

  return $c->render(template => 'qbt_add_one') unless $pick_hash;

  my $rec = $local_by_ih->{$pick_hash};
  my $source_path = (ref($rec) eq 'HASH') ? ($rec->{source_path} // '') : '';

  return $c->render(
    template => 'qbt_add_one',
    error    => "Picked $pick_hash but no source_path found",
  ) unless $source_path;

  $c->stash(picked => { hash => $pick_hash, source_path => $source_path });
  return $c->render(template => 'qbt_add_one');
});

$r->post('/qbt/add_one' => sub {
  my $c = shift;
  $c->stash(dev_mode => ($opts->{dev_mode} ? 1 : 0));

  my $hash        = lc($c->param('hash') // '');
  my $source_path = $c->param('source_path') // '';
  my $confirm     = $c->param('confirm') // '';

  # --- Basic validation ---
  if ($hash !~ /^[0-9a-f]{40}$/) {
    return $c->render(
      template => 'qbt_add_one',
      error    => "bad hash",
      picked   => { hash => $hash, source_path => $source_path },
    );
  }
  if (!$confirm) {
    return $c->render(
      template => 'qbt_add_one',
      error    => "confirm required",
      picked   => { hash => $hash, source_path => $source_path },
    );
  }
  if (!$source_path) {
    return $c->render(
      template => 'qbt_add_one',
      error    => "missing source_path",
      picked   => { hash => $hash, source_path => $source_path },
    );
  }

  my $qbt = QBTL::QBT->new(opts => {});

  # --- Preflight debug ---
  my $sz = (-e $source_path) ? (-s $source_path) : 0;
  my $ih_file = eval { _infohash_from_torrent_file($source_path) };
  $ih_file = $@ ? "(ih parse failed: $@)" : $ih_file;

  $app->log->debug("ADD_ONE preflight: sz=$sz pick_hash=$hash file_ih=$ih_file path=$source_path");

  require QBTL::SavePath;

  # --- Local cache needed to derive savepath ---
  my $local_by_ih = QBTL::LocalCache::get_local_by_ih(
    root_dir   => ($opts->{root_dir} || '.'),
    opts_local => { torrent_dir => "/" },
  );
  $local_by_ih = {} if ref($local_by_ih) ne 'HASH';

  my ($savepath, $why, $add);

  # --- Compute savepath + Add (single eval so errors bubble cleanly) ---
  my $ok = eval {
    my $rec = $local_by_ih->{$hash};
    die "no local record for $hash" unless ref($rec) eq 'HASH';

    my @sp_dbg;
($savepath, $why) = QBTL::SavePath::derive_savepath_from_payload(
  rec   => $rec,
  debug => \@sp_dbg,
);
    if (!$savepath) {
  my $dump = _fmt_savepath_debug(\@sp_dbg);
  die "savepath not found ($why)\n\n$dump";
}

    $app->log->debug("ADD_ONE savepath=$savepath why=$why");

    $add = $qbt->add_torrent_file($source_path, $savepath);
    die "add returned non-hash" unless ref($add) eq 'HASH';
    1;
  };

  if (!$ok) {
    my $err = "$@"; chomp $err;
    return $c->render(
      template => 'qbt_add_one',
      error    => $err,
      picked   => { hash => $hash, source_path => $source_path },
    );
  }

  my $body = $add->{body} // '';
  $app->log->debug("QBT add_one: ok=$add->{ok} code=$add->{code} body=$body savepath=$savepath");

  if (!$add->{ok}) {
    return $c->render(
      template => 'qbt_add_one',
      error    => "Add failed: HTTP $add->{code} $body",
      picked   => { hash => $hash, source_path => $source_path },
    );
  }

  if ($body =~ /fails/i) {
    return $c->render(
      template => 'qbt_add_one',
      error    => "qBittorrent refused add: $body",
      picked   => { hash => $hash, source_path => $source_path },
    );
  }

  # --- Force recheck (explicit, with explicit logging) ---
  my $re_ok = eval {
    $qbt->recheck_hash($hash);
    1;
  };

  if (!$re_ok) {
    my $err = "$@"; chomp $err;
    $app->log->debug("ADD_ONE recheck FAILED hash=$hash err=$err");
    return $c->render(
      template => 'qbt_add_one',
      error    => "Added OK, but recheck failed: $err",
      picked   => { hash => $hash, source_path => $source_path },
    );
  }

  $app->log->debug("ADD_ONE recheck triggered hash=$hash");

  return $c->redirect_to('/qbt/add_one');
});

$r->get('/qbt/hashnames' => sub {
  my $c = shift;
  $c->stash(dev_mode => ($opts->{dev_mode} ? 1 : 0));

  my $mode = lc($c->param('mode') // 'scroll');     # scroll | paginate
  $mode = 'scroll' unless $mode eq 'paginate';

  my $per  = int($c->param('per')  // 50);
  my $page = int($c->param('page') // 1);
  $per  = 50 if $per < 10  || $per > 500;
  $page = 1  if $page < 1;

  my $qbt = QBTL::QBT->new(opts => {});
  my $list = $qbt->get_torrents_info() || [];

  my @hits;
  for my $t (@$list) {
    next if ref($t) ne 'HASH';
    my $name = $t->{name} // '';
    next unless $name =~ /^[0-9a-fA-F]{40}$/;
    push @hits, {
      hash      => ($t->{hash} // ''),
      name      => $name,
      state     => ($t->{state} // ''),
      progress  => ($t->{progress} // -1),
      save_path => ($t->{save_path} // ''),
    };
  }

  my $found = scalar(@hits);

  my ($start, $end, @sample, $pages);
  if ($mode eq 'paginate') {
    $pages = $found ? int(($found + $per - 1) / $per) : 1;
    $page = $pages if $page > $pages;

    $start = ($page - 1) * $per;
    $end   = $start + $per - 1;
    $end   = $found - 1 if $end > $found - 1;

    @sample = ($found && $start <= $end) ? @hits[$start .. $end] : ();
  } else {
    @sample = @hits;  # scroll = all rows
    $pages  = 1;
    $start  = 0;
    $end    = $found ? ($found - 1) : 0;
  }

  $c->stash(
    found => $found,
    sample => \@sample,

    mode => $mode,
    per  => $per,
    page => $page,
    pages => $pages,
    start_n => ($found ? ($start + 1) : 0),
    end_n   => ($found ? ($end + 1)   : 0),
  );

  $c->render(template => 'qbt_hashname');
});

  $r->post('/qbt/refresh_hashname' => sub {
  my $c = shift;
  $c->stash(dev_mode => ($opts->{dev_mode} ? 1 : 0));

  return $c->render(text => "dev-mode required", status => 403)
    unless $opts->{dev_mode};

  my $hash = lc($c->param('hash') // '');
  return $c->render(text => "bad hash", status => 400)
    unless $hash =~ /^[0-9a-f]{40}$/;

  # local cache
  my $local_by_ih = QBTL::LocalCache::get_local_by_ih(
    root_dir   => ($opts->{root_dir} || '.'),
    opts_local => { torrent_dir => "/" },
  );
  $local_by_ih = {} if ref($local_by_ih) ne 'HASH';

  my $rec = $local_by_ih->{$hash};
  return $c->render(
    template => 'qbt_hashname',   # or your per-item template
    error    => "No local .torrent found for $hash",
  ) unless ref($rec) eq 'HASH' && $rec->{source_path};

  my $source_path = $rec->{source_path};

  # hard safety: the torrent file's infohash MUST match the qbt hash
  my $file_ih = eval { _infohash_from_torrent_file($source_path) };
  if ($@ || !$file_ih || $file_ih !~ /^[0-9a-f]{40}$/) {
    my $e = "$@"; chomp $e;
    return $c->render(
      template => 'qbt_hashname',
      error    => "Failed to compute infohash from torrent file: $e",
    );
  }
  if (lc($file_ih) ne $hash) {
    return $c->render(
      template => 'qbt_hashname',
      error    => "REFUSING: torrent file infohash mismatch (file=$file_ih, expected=$hash)",
    );
  }

  my $qbt = QBTL::QBT->new(opts => {});  # your new wrapper

  # refresh via add (do NOT pass savepath; let qbt keep its dl_path)
  my $add = $qbt->add_torrent_file($source_path);

  my $body = (ref($add) eq 'HASH') ? ($add->{body} // '') : '';
  $c->app->log->debug("REFRESH_HASHNAME add: code=" . ($add->{code}//'') . " body=$body");

  # even if body says Fails., the UI-equivalent behavior can still have happened
  # so we do the next step regardless unless HTTP itself failed
  if (!ref($add) || !$add->{ok}) {
    return $c->render(
      template => 'qbt_hashname',
      error    => "Add failed: HTTP " . ($add->{code}//'?') . " $body",
    );
  }

  # now that metadata is refreshed, force recheck
  eval { $qbt->recheck_hash($hash); 1 } or do {
    my $e = "$@"; chomp $e;
    return $c->render(
      template => 'qbt_hashname',
      error    => "Added/refresh attempted, but recheck failed: $e",
    );
  };

  # optional: track it
  _task_upsert($c->app, $hash,
    stage       => 'refresh_recheck',
    source_path => $source_path,
    reason      => 'hashname refresh via add + recheck',
  );

  return $c->redirect_to('/qbt/hashnames');
});

  $r->get('/qbt/repair_one' => sub {
    my $c = shift;
      $c->stash(dev_mode => ($opts->{dev_mode} ? 1 : 0));
    if (!$opts->{dev_mode}) {
      return $c->render(text => "dev-mode required", status => 403);
    }
    my $hash = lc($c->param('hash') // '');
    if ($hash !~ /^[0-9a-f]{40}$/) {
      return $c->render(text => "missing hash", status => 400);
    }
    my $local_by_ih = QBTL::LocalCache::get_local_by_ih(
      root_dir   => ($opts->{root_dir} || '.'),
      opts_local => { torrent_dir => "/" },  # TODO config
    );
    my $rec = $local_by_ih->{$hash};
    if (ref($rec) ne 'HASH' || !$rec->{source_path}) {
      return $c->render(
        template    => 'qbt_repair_one',
        error       => "No local .torrent found for $hash",
        hash        => $hash,
        source_path => '',
      );
    }
    return $c->render(
      template    => 'qbt_repair_one',
      hash        => $hash,
      source_path => $rec->{source_path},
    );
  });

  $r->post('/qbt/repair_one' => sub {
  my $c = shift;

  $c->stash(dev_mode => ($opts->{dev_mode} ? 1 : 0));

  if (!$opts->{dev_mode}) {
    return $c->render(text => "dev-mode required", status => 403);
  }

  my $hash        = lc($c->param('hash') // '');
  my $source_path = $c->param('source_path') // '';
  my $confirm     = $c->param('confirm') // '';

  if ($hash !~ /^[0-9a-f]{40}$/) {
    return $c->render(text => "bad hash", status => 400);
  }

  if (!$confirm) {
    return $c->render(text => "confirm required", status => 400);
  }

  if (!$source_path) {
    return $c->render(text => "missing source_path", status => 400);
  }

  my $qbt = QBTL::QBT->new(opts => {});

  my $exists;
  my $ok = eval {
    $exists = $qbt->torrent_exists($hash);
    1;
  };

  if (!$ok) {
    my $err = "$@";
    return $c->render(
      template    => 'qbt_repair_one',
      error       => $err,
      hash        => $hash,
      source_path => $source_path,
    );
  }

  if ($exists) {
    return $c->render(
      template    => 'qbt_repair_one',
      error       => "Torrent already exists in qBittorrent; add refused. Next step: remove/replace.",
      hash        => $hash,
      source_path => $source_path,
    );
  }

  my $add;
  $ok = eval {
    $add = $qbt->add_torrent_file_paused($source_path);
    1;
  };

  if (!$ok) {
    my $err = "$@";
    return $c->render(
      template    => 'qbt_repair_one',
      error       => $err,
      hash        => $hash,
      source_path => $source_path,
    );
  }

  my $body = $add->{body} // '';
  $app->log->debug("QBT add: ok=$add->{ok} code=$add->{code} body=$body");

  if (!$add->{ok}) {
    return $c->render(
      template    => 'qbt_repair_one',
      error       => "Add failed: HTTP $add->{code} $body",
      hash        => $hash,
      source_path => $source_path,
    );
  }

  if ($body =~ /fails/i) {
    return $c->render(
      template    => 'qbt_repair_one',
      error       => "qBittorrent refused add: $body",
      hash        => $hash,
      source_path => $source_path,
    );
  }

  return $c->redirect_to('/qbt/repairable');
});

  $r->get('/qbt/repairable' => sub {
    my $c = shift;
      $c->stash(dev_mode => ($opts->{dev_mode} ? 1 : 0));
  # --- 1) Broken list from QBT ---
    my $qbt = QBTL::QBT->new(opts => {});
    my $list = $qbt->get_torrents_info() || [];
    my @broken;
      for my $t (@$list) {
        next if ref($t) ne 'HASH';
        my $name = $t->{name} // '';
        next unless $name =~ /^[0-9a-fA-F]{40}$/;
        push @broken, {
          hash      => lc($t->{hash} // ''),
          state     => ($t->{state} // ''),
          progress  => ($t->{progress} // -1),
          save_path => ($t->{save_path} // ''),
        };
      }
  # --- 2) Local map by infohash (RAW: don't pass qbt_loaded_tor) ---
#     my $opts_local = { torrent_dir => "/" };   # TODO: make configurable
#     my $scan   = QBTL::Scan::run(opts => $opts_local);
#     my $parsed = QBTL::Parse::run(all_torrents => $scan->{torrents}, opts => $opts_local);
#     my $local_by_ih = $parsed->{by_infohash} || $parsed->{infohash_map} || {};
#       $local_by_ih = {} if ref($local_by_ih) ne 'HASH';
    my $local_by_ih = QBTL::LocalCache::get_local_by_ih(
      root_dir   => ($opts->{root_dir} || '.'),
      opts_local => { torrent_dir => "/" },  # TODO config
    );
  # --- 3) Join ---
    my @joined;
    my $repairable = 0;
    for my $b (@broken) {
      my $rec = $local_by_ih->{ $b->{hash} };
      my $have = (ref($rec) eq 'HASH') ? 1 : 0;
      $repairable++ if $have;
      push @joined, {
        %$b,
        have_local  => $have,
        source_path => ($have ? ($rec->{source_path} // '') : ''),
        bucket      => ($have ? ($rec->{bucket} // '') : ''),
      };
    }
  # DEV SAFETY CAP (TODO pagination)
    my @sample = @joined > 50 ? @joined[0..49] : @joined;
    $c->stash(
      found      => scalar(@joined),
      repairable => $repairable,
      sample     => \@sample,
    );
    $c->render(template => 'qbt_repairable');
  });

  $r->get('/qbt_state_preview' => sub {
    my $c = shift;
    my $opts = {};   # later: load config/options properly
    my $set = eval { QBTL::QBT::infohash_set(opts => $opts) };
    if ($@) {
      return $c->render(json => { error => "$@" });
    }
  # We don’t assume structure; just give a safe count.
    my $count =
      ref($set) eq 'HASH'  ? scalar(keys %$set) :
      ref($set) eq 'ARRAY' ? scalar(@$set) :
      0;
    $c->render(json => { qbt_loaded_count => $count });
  });

  $r->get('/scan' => sub {
		my $c = shift;
		my $res = QBTL::Scan::run();
		$c->render(json => $res);
	});

	$r->get('/scan_parse_preview' => sub {
    my $c = shift;
  # Minimal opts for now (we'll load real config later)
    my $opts = { torrent_dir => "/" };
    my $scan = eval { QBTL::Scan::run(opts => $opts) };
    if ($@) {
      return $c->render(json => { error => "scan: $@" });
    }
    my $parsed = eval {
      QBTL::Parse::run(all_torrents => $scan->{torrents}, opts => $opts);
    };
    if ($@) {
      return $c->render(json => { error => "parse: $@" });
    }
  # Preview: only return counts (avoid huge JSON)
    my $pending = $parsed->{pending_add} || [];
    $c->render(json => {
      found_torrents   => $scan->{found},
      pending_add      => scalar(@$pending),
      collisions_count => (
        $parsed->{collisions} ?
        scalar(keys %{ $parsed->{collisions} }) :
        0),
    });
  });

  if ($opts->{dev_mode}) {
    require QBTL::Devel;
    QBTL::Devel::register_routes($app);
    $app->log->debug("QBTL::Devel routes registered");
  }

  $r->post('/tasks/poll' => sub {
    my $c = shift;
    my $tasks = $app->defaults->{tasks} || {};
    my @hashes = sort keys %$tasks;
    my $qbt = QBTL::QBT->new(opts => {});
    my $checked = 0;
    my $resumed = 0;
    my $still   = 0;
    my $suspect = 0;
    my $missing = 0;
    for my $hash (@hashes) {
      my $t = $tasks->{$hash};
      next unless ref($t) eq 'HASH';
      my $stage = $t->{stage} || '';
      next unless $stage eq 'recheck_pending';
      $checked++;
      my $one = eval { $qbt->get_one($hash) };
      if ($@ || ref($one) ne 'HASH') {
        _task_upsert($app, $hash,
          stage  => 'missing_in_qbt',
          reason => 'not found in qbt (or query failed)',
        );
        $missing++;
        next;
      }
      my $state    = $one->{state}    // '';
      my $progress = $one->{progress} // 0;
      _task_upsert($app, $hash,
        last_state    => $state,
        last_progress => $progress,
      );
      if ($state eq 'checkingDL' || $state eq 'checkingUP') {
        $still++;
        next;
      }
      if ($progress > 0) {
      my $ok = eval { $qbt->resume($hash); 1 };
      if ($ok) {
        _task_upsert($app, $hash,
          stage  => 'resumed',
          reason => 'checking finished; progress>0',
        );
        $resumed++;
        next;
      }
      _task_upsert($app, $hash,
        stage  => 'fail_resume',
        reason => "resume failed: $@",
      );
      next;
    }
    _task_upsert($app, $hash,
      stage  => 'suspect_zero',
      reason => 'checking finished but progress=0; not auto-resuming',
    );
    $suspect++;
  }
  $c->stash(  notice => "Polled $checked pending. Resumed=$resumed StillChecking=$still
SuspectZero=$suspect Missing=$missing");
  return $c->redirect_to('/');
});
    # ---------- Idle observer tick ----------
  my $tick_seconds = 5;
  Mojo::IOLoop->recurring($tick_seconds => sub {
    _qbt_observer_tick($app, $opts);
  });
  return $app;
}

1;
