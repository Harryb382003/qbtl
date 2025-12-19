package QBTL::Web;
use common::sense;

use FindBin qw($Bin);
use Mojolicious;
use Mojo::JSON qw(true);
use QBTL::Parse;
use QBTL::Plan;
use QBTL::QBT;
use QBTL::Scan;

sub app {
  my ($opts) = @_;
    $opts ||= {};
  my $app = Mojolicious->new;

  # Serve static files from /ui
  my $ui_dir = "$Bin/../ui";
  push @{ $app->static->paths }, $ui_dir;

  my $r = $app->routes;

  $r->get('/' => sub {
    my $c = shift;
    $c->reply->static('index.html');
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
  # Your legacy parser likely has something like $parsed->{by_infohash} or similar.
  # For now: infer it safely if present.
    my $local_by_ih = $parsed->{by_infohash} || $parsed->{infohash_map} || {};
    if (ref($local_by_ih) ne 'HASH' || !keys %$local_by_ih) {
      return $c->render(json => {
        error => "No local infohash map found in parsed data (expected by_infohash/infohash_map).",
        keys_seen => [ sort grep { defined } keys %$parsed ],
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
