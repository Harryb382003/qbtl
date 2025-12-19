package QBTL::Web;
use common::sense;

use FindBin qw($Bin);
use Mojolicious;
use Mojo::JSON qw(true);

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

  return $app;
}

1;
