package QBTL::Devel;

use common::sense;

use Mojo::IOLoop;
use Mojo::JSON qw(true);
use Mojo::Util qw(md5_sum);
use Data::Dumper;
use QBTL::Utils qw(prefix_dbg);

# use lib 'lib';
# use QBTL::LocalCache;
# use QBTL::QBT;
# use QBTL::Scan;

sub register_routes {
  my ( $app, $opts ) = @_;
  $opts ||= {};

  my $r        = $app->routes;
  my $root_dir = $opts->{root_dir} || '.';

  #####################################################################
  # DEBUGGING ROUTES
  #####################################################################

  $r->get(
    '/localcache/rebuild' => sub {
      my $c = shift;

      my $tok =
          md5_sum( join '|', time, $$, rand(), $c->tx->remote_address // '' );
      $c->session( rebuild_tok => $tok );

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
      $st->{done_logged} = 0;
      $st->{done_ts}     = 0;
      $st->{last_err}    = '';
      $st->{pid}         = $$;

      delete $st->{done_logged};
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
          $st->{done_logged} = 0; # allow status endpoint to log completion once

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

  $r->get(
    '/qbt/presence_check' => sub {
      my $c = shift;

      my $qbt  = QBTL::QBT->new( {} );
      my $list = $qbt->get_torrents_info() || [];

      my ( $total, $with_hash, $name_is_hash ) = ( 0, 0, 0 );
      my @sample;

      for my $t ( @$list ) {
        next if ref( $t ) ne 'HASH';
        $total++;

        my $h = $t->{hash} // '';
        $with_hash++ if $h =~ /^[0-9a-fA-F]{40}$/;

        my $n = $t->{name} // '';
        if ( $n =~ /^[0-9a-fA-F]{40}$/ ) {
          $name_is_hash++;
          push @sample, {hash => $h, name => $n} if @sample < 5;
        }
      }

      $c->render(
                  json => {
                           total           => $total,
                           with_hash_40hex => $with_hash,
                           name_is_hash    => $name_is_hash,
                           sample          => \@sample,
                  } );
    } );

  $r->post(
    '/shutdown' => sub {
      my $c = shift;
      $c->render( text => "Shutting down..." );
      Mojo::IOLoop->next_tick( sub { Mojo::IOLoop->stop } );
    } );

  $r->get(
    '/dev_info' => sub {
      my $c = shift;
      $c->render( json => {dev_mode => true} );
    } );

  $r->get(
    '/debug_ping' => sub {
      my $c = shift;
      $c->render( text => "debug ok" );
    } );

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

  $r->get(
    '/ih_debug' => sub {
      my $c = shift;

      my $opts2 = {torrent_dir => "/"};

      my $scan = eval { QBTL::Scan::run( opts => $opts2 ) };
      if ( $@ ) { return $c->render( json => {error => "scan: $@"} ); }

      my $qbt_by_ih = eval {
        my $qbt = QBTL::QBT->new( $opts2 );
        my $h   = $qbt->get_torrents_infohash();
        $h = {} if ref( $h ) ne 'HASH';
        $h;
      };
      if ( $@ ) { return $c->render( json => {error => "qbt: $@"} ); }

      my $parsed = eval {
        QBTL::Parse::run(
                          all_torrents   => $scan->{torrents},
                          opts           => $opts2,
                          qbt_loaded_tor => $qbt_by_ih, );
      };
      if ( $@ ) { return $c->render( json => {error => "parse: $@"} ); }

      my $local_by_ih =
          $parsed->{by_infohash} || $parsed->{infohash_map} || {};

      my @local_keys = ref( $local_by_ih ) eq 'HASH' ? keys %$local_by_ih : ();
      my @qbt_keys   = ref( $qbt_by_ih ) eq 'HASH'   ? keys %$qbt_by_ih   : ();

      my $is_40hex = sub {
        my $s = shift // '';
        $s =~ s/^\s+|\s+$//g;
        return ( $s =~ /^[0-9a-fA-F]{40}$/ ) ? 1 : 0;
      };

      my $count_40hex = sub {
        my @k = @_;
        my $n = 0;
        for ( @k ) { $n++ if $is_40hex->( $_ ) }
        return $n;
      };

      my @local_sample =
          ( sort @local_keys )[ 0 .. ( @local_keys < 5 ? $#local_keys : 4 ) ];
      my @qbt_sample =
          ( sort @qbt_keys )[ 0 .. ( @qbt_keys < 5 ? $#qbt_keys : 4 ) ];

      $c->render(
                  json => {
                           local_key_count   => scalar( @local_keys ),
                           qbt_key_count     => scalar( @qbt_keys ),
                           local_40hex_count => $count_40hex->( @local_keys ),
                           qbt_40hex_count   => $count_40hex->( @qbt_keys ),
                           local_sample_keys => \@local_sample,
                           qbt_sample_keys   => \@qbt_sample,
                  } );
    } );

  $r->get(
    '/overlap_debug' => sub {
      my $c = shift;

      my $opts2 = {torrent_dir => "/"};

      my $scan = eval { QBTL::Scan::run( opts => $opts2 ) };
      if ( $@ ) { return $c->render( json => {error => "scan: $@"} ); }

      my $qbt_by_ih = eval {
        my $qbt = QBTL::QBT->new( $opts2 );
        my $h   = $qbt->get_torrents_infohash();
        $h = {} if ref( $h ) ne 'HASH';
        $h;
      };
      if ( $@ ) { return $c->render( json => {error => "qbt: $@"} ); }

      my $parsed = eval {
        QBTL::Parse::run(
                          all_torrents   => $scan->{torrents},
                          opts           => $opts2,
                          qbt_loaded_tor => $qbt_by_ih, );
      };
      if ( $@ ) { return $c->render( json => {error => "parse: $@"} ); }

      my $local_by_ih =
          $parsed->{by_infohash} || $parsed->{infohash_map} || {};
      my @hits;

      if ( ref( $local_by_ih ) eq 'HASH' && ref( $qbt_by_ih ) eq 'HASH' ) {
        for my $ih ( keys %$local_by_ih ) {
          if ( exists $qbt_by_ih->{$ih} ) {
            push @hits, $ih;
            last if @hits >= 10;
          }
        }
      }

      $c->render(
            json => {
              local_count => (
                ref( $local_by_ih ) eq 'HASH' ? scalar( keys %$local_by_ih ) : 0
              ),
              qbt_count => (
                    ref( $qbt_by_ih ) eq 'HASH' ? scalar( keys %$qbt_by_ih ) : 0
              ),
              overlap_found  => scalar( @hits ),
              overlap_sample => \@hits,
            } );
    } );

  $r->get(
    '/sample_local' => sub {
      my $c = shift;

      my $opts2 = {torrent_dir => "/"};

      my $scan = eval { QBTL::Scan::run( opts => $opts2 ) };
      if ( $@ ) { return $c->render( json => {error => "scan: $@"} ); }

      my $parsed = eval {
        QBTL::Parse::run( all_torrents => $scan->{torrents},
                          opts         => $opts2, );
      };
      if ( $@ ) { return $c->render( json => {error => "parse: $@"} ); }

      my $local_by_ih =
          $parsed->{by_infohash} || $parsed->{infohash_map} || {};
      if ( ref( $local_by_ih ) ne 'HASH' || !keys %$local_by_ih ) {
        return $c->render( json => {error => "no local infohash map found"} );
      }

      my @ih = sort keys %$local_by_ih;
      my @pick;
      for my $i ( 0 .. 19 ) {
        last if $i > $#ih;
        my $h    = $ih[$i];
        my $v    = $local_by_ih->{$h};
        my $path = ref( $v ) eq 'HASH' ? ( $v->{source_path} // '' ) : '';
        push @pick, {infohash => $h, path => $path};
      }

      $c->render( json => {sample => \@pick} );
    } );

  $r->get(
    '/local_entry' => sub {
      my $c  = shift;
      my $ih = $c->param( 'ih' ) // '';

      my $opts2 = {torrent_dir => "/"};

      my $scan = QBTL::Scan::run( opts => $opts2 );
      my $parsed = QBTL::Parse::run( all_torrents => $scan->{torrents},
                                     opts         => $opts2 );

      my $local_by_ih =
          $parsed->{by_infohash} || $parsed->{infohash_map} || {};
      my $v = ( ref( $local_by_ih ) eq 'HASH' ) ? $local_by_ih->{$ih} : undef;

      $c->render(
             json => {
               infohash => $ih,
               ref_type => ( defined $v ? ( ref( $v ) || 'SCALAR' ) : 'undef' ),
               dumped   => Dumper( $v ),
             } );
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
      my $c = shift;

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
  $r->post(
    '/queue/clear' => sub {
      my $c = shift;
      delete $c->app->defaults->{jobs};
      $c->render( text => "queue cleared\n" );
    } );
  return;
}

1;
