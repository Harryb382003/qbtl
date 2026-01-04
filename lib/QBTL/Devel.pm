package QBTL::Devel;

use common::sense;

use Mojo::IOLoop;
use Mojo::JSON qw(true);
use Data::Dumper;

# use lib 'lib';
# use QBTL::LocalCache;
# use QBTL::QBT;
# use QBTL::Scan;
# use QBTL::Parse;

sub register_routes {
  my ( $app, $opts ) = @_;
  $opts ||= {};

  my $r        = $app->routes;
  my $root_dir = $opts->{root_dir} || '.';

  #####################################################################
  # DEBUGGING ROUTES
  #####################################################################

  $r->post(
    '/localcache/rebuild' => sub {
      my $c = shift;

      # rebuild cache (writes .stor + .json)
      my ( $local_by_ih, $mtime, $src ) =
          QBTL::LocalCache::build_local_by_ih(
                                             root_dir   => $root_dir,
                                             opts_local => {torrent_dir => "/"},
          );

      # "Last Cache" should reflect the preferred cache (likely .stor)
      $c->app->defaults->{local_cache_mtime} = $mtime || time;
      $c->app->defaults->{local_cache_src}   = $src   || '';

      return $c->redirect_to( '/' );
    } );

  $r->get(
    '/localcache/rebuild' => sub {
      my $c = shift;
      return $c->render( template => 'localcache_rebuild' );
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

      my ( $local_by_ih ) =
          QBTL::LocalCache::build_local_by_ih(
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
      $app->log->debug( __LINE__ . "[Devel] /restart requested pid=$$" );

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
          $app->log->debug( __LINE__ . "[Devel] restart -> exiting 42 pid=$$" );
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
