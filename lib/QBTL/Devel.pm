package QBTL::Devel;

use common::sense;
use Data::Dumper;
use File::Spec ();
use File::Temp qw(tempfile);
use JSON::PP   ();
use Encode     qw(encode decode);
use Mojo::IOLoop;
use Mojo::JSON qw(true);
use Mojo::Util qw(md5_sum);

use QBTL::Utils qw(prefix_dbg);
use QBTL::TorrentParser;

sub register_routes {
  my ( $app, $opts ) = @_;
  $opts ||= {};

  my $r        = $app->routes;
  my $root_dir = $opts->{root_dir} || '.';

  # ---- load persisted dev prefs (file-backed) ----
  my $prefs = _prefs_load( $app );
  $app->defaults->{prefs} = $prefs;

  # canonical single flag used everywhere
  $app->defaults->{http_debug} = ( $prefs->{http_debug} ? 1 : 0 );

  #####################################################################
  # DEBUGGING ROUTES
  #####################################################################

  $r->post(
    '/debug/rebuild_local_cache' => sub {
      my $c = shift;
      $c->flash( notice => "Rebuilding local cache…" );
      my ( $local_by_ih ) =
          QBTL::LocalCache::build_local_by_ih(
                           $app,
                           root_dir => ( $c->app->defaults->{root_dir} || '.' ),
                           opts_local => {torrent_dir => "/"}, );

      $c->render(
               text => "rebuilt local cache: " . scalar( keys %$local_by_ih ) );
    } );

  $r->post(
    '/dev/prefs/http_debug' => sub {
      my $c = shift;

      my $v = $c->param( 'http_debug' );
      $v = ( $v && $v =~ /\A1\z/ ) ? 1 : 0;

      $c->app->defaults->{prefs} ||= {};
      $c->app->defaults->{prefs}{http_debug} = $v;

      # single canonical runtime flag
      $c->app->defaults->{http_debug} = $v;

      _prefs_write( $c->app, $c->app->defaults->{prefs} );

      my $back = $c->param( 'return_to' ) // '/';
      $back = '/' if $back !~ m{\A/} || $back =~ m{://} || $back =~ m{[\r\n]};
      return $c->redirect_to( $back );
    } );

  $r->get(
    '/localcache/rebuild' => sub {
      my $c = shift;

      my $tok = $c->session( 'rebuild_tok' ) // '';
      if ( !length $tok ) {
        $tok = md5_sum( join '|', time, $$, rand(),
                        ( $c->tx->remote_address // '' ) );
        $c->session( rebuild_tok => $tok );
      }

      $c->stash( rebuild_tok => $tok );
      return $c->render( template => 'localcache_rebuild' );
    } );

  $r->post(
    '/localcache/rebuild' => sub {
      my $c = shift;

      my $tok  = $c->param( 'tok' )           // '';
      my $want = $c->session( 'rebuild_tok' ) // '';

      # reject replay/invalid
      if ( !$tok || !$want || $tok ne $want ) {
        $c->flash( notice => "Rebuild already started (or token invalid)." );
        return $c->redirect_to( '/localcache/rebuild' );
      }

      # consume token
      delete $c->session->{rebuild_tok};

      my $app  = $c->app;
      my $root = $app->defaults->{root_dir} || '.';

      $app->defaults->{localcache_rebuild} ||= {};
      my $st = $app->defaults->{localcache_rebuild};

      # inflight guard (prevents actual re-runs even if browser retries POST)
      if ( $st->{inflight} ) {
        $c->flash( notice => "Rebuild already in progress…" );
        return $c->redirect_to( '/localcache/rebuild' );
      }

      $st->{inflight}    = 1;
      $st->{started_ts}  = time;
      $st->{done_ts}     = 0;
      $st->{last_err}    = '';
      $st->{pid}         = $$;
      $st->{done_logged} = 0;     # allow status endpoint to log completion once
      delete $st->{heartbeat_ts};

      $app->log->debug(
                 prefix_dbg() . " localcache rebuild START inflight=1 pid=$$" );

      # Run rebuild off-request
      Mojo::IOLoop->subprocess(
        sub {
          # child (DO NOT touch $app in here)
          QBTL::LocalCache::build_local_by_ih(
                                             $app,
                                             root_dir   => $root,
                                             opts_local => {torrent_dir => "/"},
          );
          return 1;
        },
        sub {
          # parent callback
          my ( $subp, $err, $result ) = @_;

          $st->{inflight}    = 0;
          $st->{done_ts}     = time;
          $st->{last_err}    = ( $err ? "$err" : '' );
          $st->{done_logged} = 0;

          if ( $err ) {
            $app->log->error(
                           prefix_dbg() . " localcache rebuild DONE err=$err" );
          }
          else {
            $app->log->debug( prefix_dbg() . " localcache rebuild DONE ok=1" );
          }
        } );

      # IMPORTANT: return immediately so browser doesn't retry
      $c->flash( notice => "Rebuilding local cache…" );
      return $c->redirect_to( '/localcache/rebuild' );
    } );
  $r->get(
    '/localcache/rebuild_status' => sub {
      my $c = shift;

      my $st = $c->app->defaults->{localcache_rebuild} || {};

      # completion log ONCE (when inflight just turned off)
      if ( !$st->{inflight} && ( $st->{done_ts} || 0 ) && !$st->{done_logged} )
      {
        $st->{done_logged} = 1;

        my $dt = 0;
        if ( $st->{started_ts} && $st->{done_ts} ) {
          $dt = $st->{done_ts} - $st->{started_ts};
        }

        if ( $st->{last_err} ) {
          $c->app->log->error( prefix_dbg()
                . " localcache rebuild COMPLETE err=[$st->{last_err}] dt=${dt}s"
          );
        }
        else {
          $c->app->log->debug(
                 prefix_dbg() . " localcache rebuild COMPLETE ok=1 dt=${dt}s" );
        }

        if ( $dt && $dt > 240 ) {
          $c->app->log->debug(
                          prefix_dbg() . " localcache rebuild SLOW dt=${dt}s" );
        }
      }

      # heartbeat: log at most 1x/min while inflight
      if ( $st->{inflight} ) {
        my $now  = time;
        my $last = $st->{heartbeat_ts} || 0;

        if ( ( $now - $last ) >= 60 ) {
          $st->{heartbeat_ts} = $now;

          my $dt = $st->{started_ts} ? ( $now - $st->{started_ts} ) : 0;
          $c->app->log->debug(
                      prefix_dbg() . " localcache rebuild inflight dt=${dt}s" );
        }
      }

      my $out = {
                 inflight   => ( $st->{inflight} ? 1 : 0 ),
                 started_ts => ( $st->{started_ts} || 0 ),
                 done_ts    => ( $st->{done_ts}    || 0 ),
                 last_err   => ( $st->{last_err}   || '' ),};

      return $c->render( json => $out );
    } );

  $r->post(
    '/shutdown' => sub {
      my $c = shift;
      $c->render( text => "Shutting down..." );
      Mojo::IOLoop->next_tick( sub { Mojo::IOLoop->stop } );
    } );

  $r->get(
    '/qbt_name_is_hash' => sub {
      my $c = shift;

      my $qbt = eval { QBTL::QBT->new( {} ) };
      if ( $@ ) {
        return $c->render( json => {error => "QBTL::QBT->new: $@"} );
      }

      my $list = eval { $qbt->get_torrents_info() };
      if ( $@ ) {
        return $c->render( json => {error => "fetch list: $@"} );
      }
      $list = [] if ref( $list ) ne 'ARRAY';

      my @hits;
      for my $t ( @$list ) {
        next if ref( $t ) ne 'HASH';
        my $name     = $t->{name}     // '';
        my $hash     = $t->{hash}     // $t->{infohash} // '';
        my $progress = $t->{progress} // -1;
        next unless $name =~ /^[0-9a-fA-F]{40}$/;
        push @hits,
            {
             name      => $name,
             hash      => $hash,
             save_path => ( $t->{save_path} // '' ),
             state     => ( $t->{state}     // '' ),
             progress  => $progress,};
        last if @hits >= 50;
      }

      my $zero = 0;
      $zero++ for grep { ( $_->{progress} // -1 ) == 0 } @hits;

      $c->render(
                  json => {
                           found               => scalar( @hits ),
                           found_progress_zero => $zero,
                           sample              => \@hits,
                  } );
    } );

  $r->post(
    '/restart' => sub {
      my $c          = shift;
      my $http_debug = $c->param( 'http_debug' ) ? 1 : 0;
      $c->app->defaults->{http_debug} = $http_debug;
      unless ( $c->app->defaults->{dev_mode} ) {
        return $c->render( text => "dev-mode required", status => 403 );
      }

      my $app = $c->app;

      # show we really restarted
      $app->log->debug( prefix_dbg() . "  /restart requested pid=$$" );

# --- NUKE ALL RUNTIME STATE (must use $c->app / $app, not a stray $app var) ---
      delete $app->defaults->{jobs};       # Queue.pm jobs
      delete $app->defaults->{queue};      # if you ever used this name
      delete $app->defaults->{pending};    # old pending queue
      delete $app->defaults->{tasks};      # observer tasks
      delete $app->defaults->{runtime};    # processed overlay

      delete $app->defaults->{add_one};        # add_one cursor/meta queue
      delete $app->defaults->{qbt_by_ih};      # qbt snapshot cache
      delete $app->defaults->{qbt_snap_ts};    # snapshot timestamp

      $c->stash( notice => "Restarting server… (pid=$$)" );
      $c->render( template => 'restart' );

      # Give the browser time to receive the response, then exit.
      Mojo::IOLoop->timer(
        0.15 => sub {
          $app->log->debug( prefix_dbg() . "  restart -> exiting 42 pid=$$" );
          CORE::exit( 42 );
        } );

      return;
    } );

  return;
}

sub _prefs_path {
  my ( $app ) = @_;
  my $root =
      ( ref( $app ) && ref( $app->defaults ) && $app->defaults->{root_dir} )
      ? $app->defaults->{root_dir}
      : '.';
  return File::Spec->catfile( $root, 'qbtl.cfg' );    # JSON file
}

sub _prefs_load {
  my ( $app ) = @_;
  my $path = _prefs_path( $app );

  return {} unless -e $path;

  my $raw;
  if ( open my $fh, '<:raw', $path ) {
    local $/;
    $raw = <$fh>;
    close $fh;
  }
  else {
    return {};
  }

  my $text = eval { decode( 'UTF-8', $raw, 1 ) };
  return {} if $@;

  my $h = eval { JSON::PP::decode_json( $text ) };
  return {} if $@ || ref( $h ) ne 'HASH';

  return $h;
}

sub _prefs_write {
  my ( $app, $prefs ) = @_;
  $prefs ||= {};
  return 0 unless ref( $prefs ) eq 'HASH';

  my $path = _prefs_path( $app );

  my $json = JSON::PP->new->utf8->pretty->canonical( 1 )->encode( $prefs );

  my ( $fh, $tmp ) = tempfile( "$path.XXXX", UNLINK => 0 );
  print {$fh} $json;
  close $fh;

  rename $tmp, $path or do { unlink $tmp; return 0; };

  return 1;
}

1;
