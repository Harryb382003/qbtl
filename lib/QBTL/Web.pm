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
    my $qb = QBittorrent->new({});
    $qbt_by_ih = $qb->get_torrents_infohash();   # { hash => {...} }
    $qbt_by_ih = {} if ref($qbt_by_ih) ne 'HASH';
    $qbt_list  = $qb->get_torrents_info() || []; # [ {...}, ... ]
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

  $c->stash(
    stats        => \%stats,
    stage_counts => \%stage_counts,
    fails        => \@fails,
    fmt_ts       => $fmt_ts,
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
    my $qbt_set = eval { QBTL::QBT::infohash_set(opts => $opts) };
    if ($@) {
      return $c->render(json => { error => "qbt: $@" });
    }
  # Build local ih => path map using legacy TorrentParser extract_metadata
    my $parsed = eval {
      QBTL::Parse::run(all_torrents => $scan->{torrents}, opts => $opts, qbt_loaded_tor => $qbt_set)
    };
    if ($@) {
      return $c->render(json => { error => "parse: $@" });
    }
  # We need a stable local-by-infohash structure.
    my $local_by_ih = $parsed->{by_infohash} || $parsed->{infohash_map} || {};
    if (ref($local_by_ih) ne 'HASH' || !keys %$local_by_ih) {
      return $c->render(json => {
        error => "No local infohash map found in parsed data (expected by_infohash/infohash_map).",
        keys_seen => [  grep { defined } keys %$parsed ],
      });
    }
    my $plan = QBTL::Plan::missing_infohashes(
      local_by_ih => $local_by_ih,
      qbt_set     => (ref($qbt_set) eq 'HASH' ? $qbt_set : {}),
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

  my $qbt_set = eval { QBTL::QBT::infohash_set(opts => {}) };
  if ($@) {
    return $c->render(template => 'qbt_add_one', error => "qbt set: $@");
  }
  $qbt_set = {} if ref($qbt_set) ne 'HASH';

  # pick first local hash not in qbt
  my ($pick_hash) = grep { !exists $qbt_set->{$_} }  keys %$local_by_ih;

  if (!$pick_hash) {
    return $c->render(template => 'qbt_add_one');
  }

  my $rec = $local_by_ih->{$pick_hash};
  my $source_path = (ref($rec) eq 'HASH') ? ($rec->{source_path} // '') : '';

  if (!$source_path) {
    return $c->render(template => 'qbt_add_one', error => "Picked $pick_hash but no source_path found");
  }

  $c->stash(picked => { hash => $pick_hash, source_path => $source_path });
  return $c->render(template => 'qbt_add_one');
});

$r->get('/qbt/add_one' => sub {
  my $c = shift;
    $c->stash(dev_mode => ($opts->{dev_mode} ? 1 : 0));
  my $local_by_ih = QBTL::LocalCache::get_local_by_ih(
    root_dir   => ($opts->{root_dir} || '.'),
    opts_local => { torrent_dir => "/" },
  );
  my $qb = QBittorrent->new({});
  my $qbt_by_ih = $qb->get_torrents_infohash;
    $qbt_by_ih = {} if ref($qbt_by_ih) ne 'HASH';
  my ($pick_hash) = grep { !exists $qbt_by_ih->{$_} } keys %$local_by_ih;
  if (!$pick_hash) {
    return $c->render(template => 'qbt_add_one');
  }
  my $rec = $local_by_ih->{$pick_hash};
  my $source_path = (ref($rec) eq 'HASH') ? ($rec->{source_path} // '') : '';
  if (!$source_path) {
    return $c->render(template => 'qbt_add_one', error => "Picked $pick_hash but no source_path found");
  }
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
    return $c->render(template => 'qbt_add_one',
      error  => "bad hash",
      picked => { hash => $hash, source_path => $source_path },
    );
  }
  if (!$confirm) {
    return $c->render(template => 'qbt_add_one',
      error  => "confirm required",
      picked => { hash => $hash, source_path => $source_path },
    );
  }
  if (!$source_path) {
    return $c->render(template => 'qbt_add_one',
      error  => "missing source_path",
      picked => { hash => $hash, source_path => $source_path },
    );
  }

  my $qb = QBittorrent->new({});

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

  my ($savepath, $why, $add);

  # --- Compute savepath (no side effects) ---
  my $ok = eval {
    my $rec = $local_by_ih->{$hash};
    die "no local record for $hash" unless ref($rec) eq 'HASH';

    ($savepath, $why) = QBTL::SavePath::derive_savepath_from_payload(rec => $rec);
    1;
  };

  if (!$ok) {
    my $err = "$@"; chomp $err;
    _task_upsert($app, $hash,
      stage       => 'error',
      reason      => $err,
      source_path => $source_path,
    );
    return $c->render(template => 'qbt_add_one',
      error  => $err,
      picked => { hash => $hash, source_path => $source_path },
    );
  }

  # --- If we can’t derive savepath, track as unadded and stop (by design) ---
  if (!$savepath) {
    my $msg = "savepath not found ($why)";
    _task_upsert($app, $hash,
      stage       => 'unadded',
      reason      => $msg,
      source_path => $source_path,
    );
    return $c->render(template => 'qbt_add_one',
      error  => $msg,
      picked => { hash => $hash, source_path => $source_path },
    );
  }

  $app->log->debug("ADD_ONE savepath=$savepath why=$why");

  # --- Add torrent ---
  $add = $qb->add_torrent_file($source_path, $savepath);

  my $body = $add->{body} // '';
  $app->log->debug("QBT add_one: ok=$add->{ok} code=$add->{code} body=$body savepath=$savepath");

  if (!$add->{ok} || $body =~ /fails/i) {
    my $msg = $add->{ok}
      ? "qBittorrent refused add: $body"
      : "Add failed: HTTP $add->{code} $body";

    _task_upsert($app, $hash,
      stage       => 'add_failed',
      reason      => $msg,
      source_path => $source_path,
      savepath    => $savepath,
    );

    return $c->render(template => 'qbt_add_one',
      error  => $msg,
      picked => { hash => $hash, source_path => $source_path },
    );
  }

  # --- Force recheck ---
  my $rok = eval { $qb->recheck_hash($hash); 1; };
  if (!$rok) {
    my $err = "$@"; chomp $err;
    my $msg = "Added OK, but recheck failed: $err";

    _task_upsert($app, $hash,
      stage       => 'recheck_failed',
      reason      => $msg,
      source_path => $source_path,
      savepath    => $savepath,
    );

    return $c->render(template => 'qbt_add_one',
      error  => $msg,
      picked => { hash => $hash, source_path => $source_path },
    );
  }

  _task_upsert($app, $hash,
    stage       => 'recheck_requested',
    reason      => "added ok; recheck requested (why=$why)",
    source_path => $source_path,
    savepath    => $savepath,
  );

  return $c->redirect_to('/qbt/add_one');
});

  $r->get('/qbt/broken' => sub {
  my $c = shift;
    $c->stash(dev_mode => ($opts->{dev_mode} ? 1 : 0));
  my $qb = QBittorrent->new({});
  my $list = $qb->get_torrents_info() || [];

  my @hits;
  for my $t (@$list) {
    next if ref($t) ne 'HASH';
    my $name = $t->{name} // '';
    next unless $name =~ /^[0-9a-fA-F]{40}$/;
    push @hits, {
      hash      => ($t->{hash} // ''),
      save_path => ($t->{save_path} // ''),
      state     => ($t->{state} // ''),
    };
  }
  my @sample = @hits > 10 ? @hits[0..9] : @hits;
  $c->stash(found => scalar(@hits), sample => \@sample);
  $c->render(template => 'qbt_broken');
  });

  $r->get('/qbt/hashnames' => sub {
    my $c = shift;
      $c->stash(dev_mode => ($opts->{dev_mode} ? 1 : 0));
    my $qb = QBittorrent->new({});
    my $list = $qb->get_torrents_info() || [];
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
    my @sample = @hits > 50 ? @hits[0..49] : @hits;
    $c->stash(found => scalar(@hits), sample => \@sample);
    $c->render(template => 'qbt_hashname');
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

  my $qb = QBittorrent->new({});

  my $exists;
  my $ok = eval {
    $exists = $qb->torrent_exists($hash);
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
    $add = $qb->add_torrent_file_paused($source_path);
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
    my $qb = QBittorrent->new({});
    my $list = $qb->get_torrents_info() || [];
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


  return $app;
}

1;
