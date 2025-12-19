package QBTL::Web;
use common::sense;

use FindBin qw($Bin);
use Mojolicious;
use Mojo::JSON qw(true);
use QBTL::Scan;
use QBTL::Parse;


sub app {
  my $app = Mojolicious->new;

  # Serve static files from /ui
  my $ui_dir = "$Bin/../ui";
  push @{ $app->static->paths }, $ui_dir;

  my $r = $app->routes;

  $r->get('/' => sub {
    my $c = shift;
    $c->reply->static('index.html');
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

  $r->get('/scan' => sub {
		my $c = shift;
		my $res = QBTL::Scan::run();
		$c->render(json => $res);
	});

  return $app;
}

1;
