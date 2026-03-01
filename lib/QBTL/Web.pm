package QBTL::Web;
use common::sense;

use Digest::SHA qw(sha1_hex);
use File::Spec;
use File::Basename qw(dirname basename);
use FindBin        qw($Bin);
use Mojolicious;
use Mojo::JSON         qw(true);
use Mojo::Util         qw(md5_sum);
use Scalar::Util       qw(refaddr);
use Unicode::Normalize qw(NFC NFD);

use QBTL::LocalCache qw( get_local_by_ih );
use QBTL::Logger;
use QBTL::QBT      qw( qbt_echo );
use QBTL::Classify qw (
    classify_triage
    classify_no_hits
);
use QBTL::Queue;
use QBTL::SavePath qw(
    munge_savepath_and_root_rename
    torrent_top_lvl_from_rec
    derive_savepath_from_payload
);
use QBTL::Store qw( store_put_qbt_snapshot );
use QBTL::Utils qw(
    start_timer
    stop_timer
    short_ih
    prefix_dbg
);

QBTL::Logger::enable_disk_logging( path => 'qbtl.log', level => 'debug' );

BEGIN {
  my $repo_root = File::Spec->catdir( $Bin, '..' );    # bin/.. => repo root
  my $log_path =
      File::Spec->catfile( $repo_root, 'qbtl.log' );    # repo_root/qbtl.log
  QBTL::Logger::enable_disk_logging( path => $log_path, level => 'debug' );
}

sub app {
  my ( $opts ) = @_;
  $opts ||= {};
  my $app = Mojolicious->new;
  $app->defaults->{health} ||= {};
  $app->defaults->{health}{qbt} =
      qbt_echo(
                $app,
                port     => 8080,
                want_api => 0 );
  $app->defaults->{store} ||= {
                               by_ih   => {},
                               classes => {},
                               runtime => {},};
  QBTL::Logger::set_log_file( $opts->{log_file} || 'qbtl.log' );
  $app->log->level( 'debug' );

  $app->defaults->{dev_mode} = ( $opts->{dev_mode} ? 1 : 0 );

  my $root = $opts->{root_dir} || '.';
  $app->defaults->{root_dir} = $root;

  # Defaults for cache metadata
  $app->defaults->{local_cache_mtime} = 0;
  $app->defaults->{local_cache_src}   = '';

  # Keep cache stamp fresh (prefer .stor else .json)
  push @{$app->renderer->paths}, File::Spec->catdir( $root, 'templates' );
  push @{$app->static->paths},   File::Spec->catdir( $root, 'ui' );

  $app->hook(
    before_dispatch => sub {
      my $c = shift;

      # --- CONFIRM MOUNT POINTS
      $c->app->defaults->{runtime} ||= {};
      my $rt = $c->app->defaults->{runtime};

      my $mounted = QBTL::Utils::mounted_vols_map();
      my $fp      = QBTL::Utils::mounts_fingerprint( $mounted );

      my $prev_fp = $rt->{mounts_fp} // '';
      $rt->{mounts} ||= {};
      $rt->{mounts}{ts}      = time;
      $rt->{mounts}{mounted} = $mounted;
      $rt->{mounts_fp}       = $fp;

      # if mounts changed, auto-clear ignore_all
      if ( length( $prev_fp ) && $prev_fp ne $fp ) {
        $rt->{ignore_unmounted_all} = 0;
        $rt->{mounts_changed_ts}    = time;
      }

      # --- QBT HEALTH (cheap every request; API only when needed+stale) ---
      $c->app->defaults->{health} ||= {};

      my $qh = $c->app->defaults->{health}{qbt} ||= {

        #                                                      qbt_up   => 0,
        api_ok   => 0,
        echo_ts  => time,
        echo_err => '',
        want_api => 0,
        pid      => 0,};

      # always cheap (proc-only)
      $qh = QBTL::QBT::qbt_echo( $app );
      $c->app->defaults->{health}{qbt} = $qh;

      # decide if this route needs API
      my $path = $c->req->url->path->to_string // '';
      my $needs_api =
             ( $path =~ m{\A/qbt} )
          || ( $path =~ m{\A/queue} )
          || ( $path =~ m{\A/triage} )
          || ( $path =~ m{\A/localcache} );

      # stale if older than 60s (or never set)
      my $stale = 1;
      if ( $qh->{echo_ts} ) {
        $stale = ( time - $qh->{echo_ts} ) > 60 ? 1 : 0;    # 60s

        #         $app->log->debug( prefix_dbg() . " stale: " . $stale );
      }

      # Only then hit API
      if ( $needs_api && $stale ) {
        $qh = QBTL::QBT::qbt_echo( $app, want_api => 1 );
        $c->app->defaults->{health}{qbt} = $qh;
      }

      my $pid = $qh->{pid} || 0;
      my $err = $qh->{echo_err} // $qh->{err} // '';

 #       $c->app->log->debug(
 #                                  prefix_dbg()
 #                                . " stale: "
 #                                . $stale
 #                                . ( $pid ? " QBT UP pid: $pid" : " QBT DOWN" )
 #                                . (
 #                                    length( $err )
 #                                    ? " err: [$err]"
 #                                    : "" ) );

      # --- /QBT HEALTH ---

      # --- PROVE REQUESTS ARE HITTING MOJO ---
      my $m = $c->req->method                // '';
      my $q = $c->req->url->query->to_string // '';
      my $p = $c->req->url->path->to_string  // '';

      # --- throttle noisy polling endpoints ---
      my $log_req = 1;

      if ( $p eq '/localcache/rebuild_status' ) {
        my $st   = $c->app->defaults->{localcache_rebuild} ||= {};
        my $now  = time;
        my $last = $st->{reqlog_ts} || 0;

        # log at most once per minute
        if ( ( $now - $last ) < 60 ) {
          $log_req = 0;
        }
        else {
          $st->{reqlog_ts} = $now;
        }
      }

      if ( $log_req ) {
        $c->app->log->debug(
                              prefix_dbg()
                            . " REQ $m $p"
                            . ( length( $q ) ? "?$q" : "" )
                            . " stale: "
                            . $stale
                            . ( $pid ? " QBT is UP pid: $pid" : " QBT is DOWN" )
                            . (
                                length( $err )
                                ? " err: [$err]"
                                : "" )
                            . "\n" );
      }
      my $root = $c->app->defaults->{root_dir} || '.';

      my $bin  = QBTL::LocalCache::cache_path_bin( root_dir => $root );
      my $json = QBTL::LocalCache::cache_path_json( root_dir => $root );

      my ( $ts, $src ) =
            -e $bin  ? ( ( stat( $bin ) )[9]  || 0, 'bin' )
          : -e $json ? ( ( stat( $json ) )[9] || 0, 'json' )
          :            ( 0, '' );

      $c->app->defaults->{local_cache_mtime} = $ts;
      $c->app->defaults->{local_cache_src}   = $src;

      $c->stash(
            local_cache_mtime => $ts,
            local_cache_src   => $src,
            local_cache_label => ( $ts ? scalar( localtime( $ts ) ) : 'never' ),
      );
    } );

  # Serve static files from /ui
  my $ui_dir = "$Bin/../ui";
  push @{$app->static->paths}, $ui_dir;

  my $r = $app->routes;

  if ( $opts->{dev_mode} ) {
    require QBTL::Devel;
    QBTL::Devel::register_routes( $app, $opts );    # <-- IMPORTANT
    $app->log->debug( prefix_dbg() . " DEVELOPER ROUTES REGISTERED" );
  }

  $r->get(
    '/api/torrent_tree/:ih' => sub {
      my $c  = shift;
      my $ih = lc( $c->param( 'ih' ) // '' );
      return $c->render( json => {ok => 0, err => 'bad ih'}, status => 400 )
          unless $ih =~ /^[0-9a-f]{40}$/;

      my $h = $c->app->defaults->{local_by_ih} || {};
      $h = {} if ref( $h ) ne 'HASH';

      my $rec = $h->{$ih} || {};
      $rec = {} if ref( $rec ) ne 'HASH';

      my $files = $rec->{files};
      $files = [] if ref( $files ) ne 'ARRAY';

      # Return only what UI needs (keep it small)
      return $c->render(
        json => {
          ok    => 1,
          ih    => $ih,
          name  => ( $rec->{name} // '' ),
          files => [
            map {
              ( ref( $_ ) eq 'HASH' )
                  ? {
                     path   => ( $_->{path}   // '' ),
                     length => ( $_->{length} // 0 )}
                  : ()
            } @$files
          ],
        } );
    } );

  $r->get(
    '/' => sub {
      my $c = shift;
      $c->stash( dev_mode => ( $opts->{dev_mode} ? 1 : 0 ) );

      # ---------- Load local cache ----------
      my ( $local_by_ih, $mtime, $src ) =
          QBTL::LocalCache::get_local_by_ih(
                                       $app,
                                       root_dir => ( $opts->{root_dir} || '.' ),
                                       opts_local => {torrent_dir => "/"}, );
      $local_by_ih = {} if ref( $local_by_ih ) ne 'HASH';

      my $store = $app->defaults->{store};

      for my $ih ( keys %$local_by_ih ) {
        next unless $ih =~ /^[0-9a-f]{40}$/;
        next unless ref( $local_by_ih->{$ih} ) eq 'HASH';

        $store->{by_ih}{$ih} ||= {ih => $ih};
        @{$store->{by_ih}{$ih}}{keys %{$local_by_ih->{$ih}}} =
            values %{$local_by_ih->{$ih}};
      }

      $c->stash( local_cache_mtime => ( $mtime || 0 ) );

      # --- Make rebuild token available for navbar form on ALL pages ---
      my $tok = $c->session( 'rebuild_tok' ) // '';
      if ( !length $tok ) {
        $tok = md5_sum( join '|', time, $$, rand(),
                        ( $c->tx->remote_address // '' ) );
        $c->session( rebuild_tok => $tok );
      }
      $c->stash( rebuild_tok     => $tok );
      $c->stash( local_cache_src => ( $src || '' ) );

      my $local_count = scalar( keys %$local_by_ih );

      # ---------- Load qBittorrent state ----------
      my ( $qbt_by_ih, $qbt_list );
      my $qbt_err = '';
      eval {
        my $qbt = QBTL::QBT->new( {app => $c->app} );
        $qbt_by_ih = $qbt->api_torrents_infohash_map();
        store_put_qbt_snapshot( $c->app, $qbt_by_ih );
        $qbt_by_ih = {} if ref( $qbt_by_ih ) ne 'HASH';
        $qbt_list  = $qbt->api_torrents_info() || [];
        1;
          }
          or do {
        $qbt_err   = "$@";
        $qbt_by_ih = {};
        $qbt_list  = [];
          };

      my $qbt_loaded_count = scalar( keys %$qbt_by_ih );

      my $name_is_ih = 0;
      my $repairable = 0;

      for my $t ( @$qbt_list ) {
        next if ref( $t ) ne 'HASH';
        my $name = $t->{name} // '';
        next unless $name =~ /^[0-9a-fA-F]{40}$/;
        $name_is_ih++;

        my $ih = $t->{ih} // '';
        next unless $ih =~ /^[0-9a-f]{40}$/;
        $repairable++ if exists $local_by_ih->{$ih};
      }

      my $missing_from_qbt = 0;
      for my $ih ( keys %$local_by_ih ) {
        $missing_from_qbt++ if !exists $qbt_by_ih->{$ih};
      }

      my %stage_counts;
      my @fails;

      for my $ih ( keys %$local_by_ih ) {
        my $rec = $local_by_ih->{$ih};
        next if ref( $rec ) ne 'HASH';

        my $rt = $rec->{runtime};
        next if ref( $rt ) ne 'HASH';

        my $stage = $rt->{stage} // 'unknown';
        $stage_counts{$stage}++;

        next unless $stage eq 'unadded' || $stage eq 'error';

        push @fails,
            {
             ih            => $ih,
             name          => ( $rec->{name} // '' ),
             stage         => $stage,
             reason        => ( $rt->{reason}     // '' ),
             last_state    => ( $rt->{last_state} // '' ),
             last_progress => (
                        defined $rt->{last_progress} ? $rt->{last_progress} : ''
             ),
             ts          => ( $rt->{ts}           // 0 ),
             source_path => ( $rec->{source_path} // '' ),};
      }

      @fails = sort { ( $b->{ts} || 0 ) <=> ( $a->{ts} || 0 ) } @fails;

      my $fmt_ts = sub {
        my ( $ts ) = @_;
        return '' unless $ts;
        my @lt = localtime( $ts );
        return
            sprintf( "%04d-%02d-%02d %02d:%02d:%02d",
                     $lt[5] + 1900,
                     $lt[4] + 1,
                     $lt[3], $lt[2], $lt[1], $lt[0] );
      };

      my %stats = (
                    local_unique     => $local_count,
                    qbt_loaded       => $qbt_loaded_count,
                    qbt_name_is_ih   => $name_is_ih,
                    missing_from_qbt => $missing_from_qbt,
                    repairable       => $repairable,
                    qbt_error        => $qbt_err, );

      my $tick = $app->defaults->{observer_last_tick} || 0;

      $c->stash(
        stats        => \%stats,
        stage_counts => \%stage_counts,
        fails        => \@fails,
        fmt_ts       => $fmt_ts,

        observer_last_tick_h => _fmt_ts( $tick ),
        observer_last_count  => ( $app->defaults->{observer_last_count} || 0 ),
      );

      $c->render( template => 'index' );
    } );

  $r->get(
    '/qbt/add_one' => sub {
      my $c = shift;
      $c->app->log->debug(   prefix_dbg()
                           . "  add_one context method="
                           . $c->req->method
                           . " path="
                           . $c->req->url->path . " ref="
                           . ( $c->req->headers->referrer // '(none)' )
                           . " return_to="
                           . ( $c->param( 'return_to' ) // '(none)' ) . " ih="
                           . ( $c->param( 'ih' )        // '(none)' ) );
      $c->app->log->debug(   prefix_dbg()
                           . "  add_one GET return_to_in="
                           . ( $c->param( 'return_to' ) // '(none)' ) );
      $c->app->log->debug(   prefix_dbg()
                           . "  add_one GET return_to_ok: "
                           . ( $c->stash( 'return_to' ) // '(none)' ) );

      $c->stash( dev_mode => ( $opts->{dev_mode} ? 1 : 0 ) );

      my $return_to = $c->param( 'return_to' ) // '';

# allow only local paths; reject absolute URLs and anything with whitespace/newlines
      if (    $return_to !~ m{\A/}
           || $return_to =~ m{://}
           || $return_to =~ m{[\r\n]} )
      {
        $return_to = '/qbt/add_one';
      }

      $c->stash( return_to => $return_to );

      # OK click? leave this screen (optionally bump first)
      my $ok = $c->param( 'ok' ) // '';
      if ( $ok )    # bump semantics, OK == move on
      {
        _add_one_advance( $c->app );

        $c->app->log->debug( prefix_dbg() . " queued return_to=$return_to" );
        return $c->redirect_to( $return_to );
      }

      my ( $local_by_ih, $cache_mtime, $cache_src ) =
          get_local_by_ih(
                           $app,
                           root_dir   => ( $opts->{root_dir} || '.' ),
                           opts_local => {torrent_dir => "/"}, );
      $local_by_ih = {} if ref( $local_by_ih ) ne 'HASH';

      # Pull qbt snapshot ONLY when we need to (queue build / rebuild)
      my $qbt       = QBTL::QBT->new( {app => $c->app} );
      my $qbt_by_ih = $qbt->api_torrents_infohash_map;
      store_put_qbt_snapshot( $c->app, $qbt_by_ih );
      $qbt_by_ih = {} if ref( $qbt_by_ih ) ne 'HASH';

      my $st = _add_one_state( $c->app );
      $c->app->log->debug(   prefix_dbg()
                           . " [add_one GET] queue_n="
                           . scalar( @{$st->{queue} || []} ) . " idx="
                           . ( $st->{idx} // 0 )
                           . " meta_n="
                           . scalar( keys %{$st->{meta} || {}} ) );

      # Rebuild queue if empty OR cache has changed since queue build
      if ( !@{$st->{queue} || []}
           || ( ( $st->{cache_mtime} || 0 ) != ( $cache_mtime || 0 ) ) )
      {
        _add_one_build_queue(
                              app         => $c->app,
                              local_by_ih => $local_by_ih,
                              qbt_by_ih   => $qbt_by_ih,
                              cache_mtime => ( $cache_mtime || 0 ), );
      }

      # Explicit "next" click?
      my $next = $c->param( 'next' ) // '';
      if ( $next ) {
        _add_one_advance( $c->app );
      }

      # Optional explicit ih (?ih=...)
      my $want_ih = $c->param( 'ih' ) // '';
      $want_ih =~ s/\s+//g;
      $want_ih = '' unless $want_ih =~ /^[0-9a-f]{40}$/;

      my $pick_ih;

      if ( $want_ih ) {
        $pick_ih = $want_ih;

        unless ( exists $local_by_ih->{$pick_ih} ) {
          return
              $c->render(
                     template => 'qbt_add_one',
                     error => "Requested ih not found in local cache: $want_ih",
                     return_to => $return_to, );
        }

        if ( exists $qbt_by_ih->{$pick_ih} ) {
          return
              $c->render(
            template => 'qbt_add_one',
            error    =>
"Requested ih already exists in qBittorrent: \n\t$pick_ih\n\t$want_ih",
            return_to => $return_to, );
        }
      }
      else {
        $pick_ih = $st->{queue}[ $st->{idx} ] if @{$st->{queue} || []};
      }

      return
          $c->render(
                      template  => 'qbt_add_one',
                      return_to => $return_to,
          ) unless $pick_ih;

      my $rec = $local_by_ih->{$pick_ih};
      my $source_path =
          ( ref( $rec ) eq 'HASH' ) ? ( $rec->{source_path} // '' ) : '';

      if ( !$source_path ) {
        return
            $c->render(
                        template  => 'qbt_add_one',
                        error     => "Picked $pick_ih but no source_path found",
                        return_to => $return_to, );
      }

      my $meta = _add_one_state( $c->app )->{meta}{$pick_ih} || {};

      $c->stash(
                 picked => {
                            ih          => $pick_ih,
                            source_path => $source_path,
                 },
                 add_one_meta    => $meta,
                 add_one_queue_n =>
                     scalar( @{_add_one_state( $c->app )->{queue} || []} ),
                 add_one_idx => ( _add_one_state( $c->app )->{idx} || 0 ) + 1,
                 return_to   => $return_to, );

      return $c->render( template => 'qbt_add_one' );
    } );

  $r->post(
    '/qbt/add_one' => sub {

      # ----- init
      my $c = shift;
      $c->stash( dev_mode => ( $opts->{dev_mode} ? 1 : 0 ) );

      my $return_to = $c->param( 'return_to' ) // '';
      $return_to =~ s/^\s+|\s+$//g;

# allow only local paths; reject absolute URLs and anything with whitespace/newlines
      if (    $return_to !~ m{\A/}
           || $return_to =~ m{://}
           || $return_to =~ m{[\r\n]} )
      {
        $return_to = '/qbt/add_one';
      }

      my $want_redirect_back = ( $return_to ne '/qbt/add_one' ) ? 1 : 0;

      my $fail = sub {
        my ( $msg, %extra ) = @_;
        $msg ||= 'error';

        # if coming from Page View / popup flow, prefer redirect back + flash
        if ( $want_redirect_back ) {
          $c->flash( notice => $msg );
          return $c->redirect_to( $return_to );
        }

        # otherwise, classic add_one screen
        return
            $c->render(
                        template => 'qbt_add_one',
                        error    => $msg,
                        picked   => {
                                   ih          => ( $extra{ih}          // '' ),
                                   source_path => ( $extra{source_path} // '' ),
                        },
                        return_to => $return_to, );
      };

      my $ih = lc( $c->param( 'ih' ) // '' );
      $ih =~ s/\s+//g;

      my $confirm = $c->param( 'confirm' ) // '';
      my $retry   = $c->param( 'retry' )   // 0;    # hook only (unused for now)

      unless ( $ih =~ /^[0-9a-f]{40}$/ ) {
        return
            $fail->(
                     "BAD INFOHASH",
                     ih          => $ih,
                     source_path => ( $c->param( 'source_path' ) // '' ) );
      }

      unless ( $confirm ) {
        return
            $fail->(
                     "confirm required",
                     ih          => $ih,
                     source_path => ( $c->param( 'source_path' ) // '' ) );
      }

      # ----------------------------
      # source_path: param first, then cache fallback by ih
      # ----------------------------
      my $source_path = $c->param( 'source_path' ) // '';
      $source_path =~ s/^\s+|\s+$//g;

      # Prefer the canonical in-memory snapshot if you have it
      my $by_ih = $c->app->defaults->{localcache}{by_ih};
      $by_ih = {} if ref( $by_ih ) ne 'HASH';

      if ( !length $source_path ) {
        my $rec = $by_ih->{$ih};
        if ( ref( $rec ) eq 'HASH' ) {
          $source_path = $rec->{source_path} // '';
          $source_path =~ s/^\s+|\s+$//g;
        }
      }

      # If still missing, fall back to disk cache load (slower, but correct)
      if ( !length $source_path ) {
        my ( $local_by_ih ) =
            QBTL::LocalCache::get_local_by_ih(
                                       $c->app,
                                       root_dir => ( $opts->{root_dir} || '.' ),
                                       opts_local => {torrent_dir => "/"}, );
        $local_by_ih = {} if ref( $local_by_ih ) ne 'HASH';

        my $rec = $local_by_ih->{$ih};
        if ( ref( $rec ) eq 'HASH' ) {
          $source_path = $rec->{source_path} // '';
          $source_path =~ s/^\s+|\s+$//g;
        }
      }

      $app->log->debug( prefix_dbg() . " SOURCE_PATH: $source_path\n " );

      unless ( length $source_path ) {
        return
            $fail->(
                     "missing source_path",
                     ih          => $ih,
                     source_path => $source_path );
      }

      # ---------- Runtime overlay for Page View culling ----------
      $c->app->defaults->{runtime} ||= {};
      $c->app->defaults->{runtime}{processed} ||= {};
      my $processed = $c->app->defaults->{runtime}{processed};

      my $qbt = QBTL::QBT->new( {app => $c->app} );

# --- Pre-check: already exists in qBittorrent? (stale queue / already added) ---
      my $exists = eval {
        my $t = $qbt->api_torrents_info_one( $ih );
        ( ref( $t ) eq 'HASH' && ( $t->{ih} // '' ) =~ /^[0-9a-f]{40}$/ )
            ? 1
            : 0;
      };

      if ( $@ ) {
        my $err = "$@";
        chomp $err;

        _add_one_mark_fail( $c->app, $ih, "torrent_exists check failed: $err" );

        $processed->{$ih} = {
                             status      => 'error',
                             ts          => time,
                             last_error  => "torrent_exists check failed: $err",
                             source_path => $source_path,};

        _add_one_remove_ih( $c->app, $ih );
        my $st = _add_one_state( $c->app );
        $app->log->debug( prefix_dbg()
                   . " [add_one] after remove ih: $ih idx: $st->{idx} queue_n: "
                   . scalar( @{$st->{queue} || []} ) );

        return
            $fail->(
                     "torrent_exists check failed: $err",
                     ih          => $ih,
                     source_path => $source_path );
      }

      if ( $exists ) {

     # If it's an infohash-as-name torrent, "allow" by routing to the fix action
        my $t        = $qbt->api_torrents_info_one( $ih );
        my $qbt_name = ( ref( $t ) eq 'HASH' ) ? ( $t->{name} // '' ) : '';

        if ( length( $qbt_name ) && $qbt_name eq $ih ) {
          return $c->redirect_to( "/qbt/hashname?hash=$ih" );
        }

        my $msg =
            "Already exists in qBittorrent (stale queue or previously added):"
            . $ih;

        _add_one_mark_fail( $c->app, $ih, $msg );

        $processed->{$ih} = {
                             status      => 'exists',
                             ts          => time,
                             last_error  => $msg,
                             source_path => $source_path,};

        _add_one_remove_ih( $c->app, $ih );
        my $st = _add_one_state( $c->app );
        $app->log->debug(   prefix_dbg()
                          . " AFTER REMOVE ih: $ih idx: $st->{idx} queue_n: "
                          . scalar( @{$st->{queue} || []} ) );

        return $fail->( $msg, ih => $ih, source_path => $source_path );
      }

      my $sz = ( -e $source_path ) ? ( -s $source_path ) : 0;

      my $rt = $c->app->defaults->{runtime} || {};
      my $mounted =
          ( $rt->{mounts} && ref( $rt->{mounts}{mounted} ) eq 'HASH' )
          ? $rt->{mounts}{mounted}
          : {};

      my $v_torrent = QBTL::Utils::vol_from_path( $source_path );
      if ( length( $v_torrent ) && !$mounted->{$v_torrent} ) {
        my $err =
            "Torrent volume not mounted: $v_torrent (source_path=$source_path)";
        _add_one_mark_fail( $c->app, $ih, $err );

        classify_triage(
                         $c->app,
                         {
                          ih          => $ih,
                          key         => 'missing_mount_torrent',
                          path        => '/qbt/add_one',
                          err         => $err,
                          source_path => $source_path
                         },
                         'missing_mount' );

        _add_one_remove_ih( $c->app, $ih );
        return $fail->( $err, ih => $ih, source_path => $source_path );
      }

      my $ih_file = eval { _infohash_from_torrent_file( $app, $source_path ) };
      $ih_file = $@ ? "(ih parse failed: $@)" : $ih_file;

      $app->log->debug(   prefix_dbg()
                        . " PREFLIGHT: \n\tsize:$sz pick_ih: "
                        . short_ih( $ih )
                        . " file_ih: "
                        . short_ih( $ih_file )
                        . "\n\tpath: $source_path" );

      require QBTL::SavePath;

      # Use the best local_by_ih we have (prefer in-memory canonical)
      my $local_by_ih = $by_ih;
      if ( ref( $local_by_ih ) ne 'HASH' || !%$local_by_ih ) {
        my ( $disk_by_ih ) =
            QBTL::LocalCache::get_local_by_ih(
                                       $app,
                                       root_dir => ( $opts->{root_dir} || '.' ),
                                       opts_local => {torrent_dir => "/"}, );
        $disk_by_ih  = {} if ref( $disk_by_ih ) ne 'HASH';
        $local_by_ih = $disk_by_ih;
      }

      my ( $savepath, $why, $add );
      my $pending_root_rename_data;

      my $ok = eval {
        my $rec = $local_by_ih->{$ih};
        die "no local record for $ih" unless ref( $rec ) eq 'HASH';

        my @sp_dbg;
        ( $savepath, $why ) =
            QBTL::SavePath::derive_savepath_from_payload(
                                                          $app, $ih_file,
                                                          rec   => $rec,
                                                          debug => \@sp_dbg, );

        unless ( $savepath ) {

          # minimal record for NO_HITS (stash whatever you have)
          my $nohit = {
            ih          => $ih,
            name        => ( $rec->{name}        // '' ),
            bucket      => ( $rec->{bucket}      // '' ),
            tracker     => ( $rec->{tracker}     // '' ),
            source_path => ( $rec->{source_path} // '' ),
            total_size  => ( $rec->{total_size}  // 0 ),
            files => ( ref( $rec->{files} ) eq 'ARRAY' ? $rec->{files} : [] ),

            savepath_why => ( $why // '' ),
            savepath_dbg => \@sp_dbg,};

          classify_no_hits( $app, $nohit, "savepath_not_found" );
          _add_one_mark_fail( $c->app, $ih, "NO_HITS: savepath_not_found" );

          _add_one_remove_ih( $c->app, $ih );
          my $st = _add_one_state( $c->app );
          $app->log->debug( prefix_dbg()
                   . " [add_one] NO_HITS removed ih=$ih idx=$st->{idx} queue_n="
                   . scalar( @{$st->{queue} || []} ) );

          # bounce back (caller-driven)
          return $c->redirect_to( $return_to );
        }

        # ---- Extract hit_path from debug ----
        my $hit_path = '';
        for my $ln ( @sp_dbg ) {
          next unless defined $ln;
          if ( ref( $ln ) eq 'HASH' ) {next}    # just in case
          if ( $ln =~ /\bhit=(.+)\z/ ) {
            $hit_path = $1;
            $hit_path =~ s/\s+\z//;
            last;
          }
        }
        if ( !$hit_path && defined $why && $why =~ /\bhit=(.+)\z/ ) {
          $hit_path = $1;
          $hit_path =~ s/\s+\z//;
        }

        $app->log->debug(
                 prefix_dbg() . " dbg hit_path=" . ( $hit_path || '(empty)' ) );

        my $torrent_top_dbg = torrent_top_lvl_from_rec( $rec );
        my $is_multi_dbg    = _is_multi_file_torrent( $rec ) ? 1 : 0;
        $app->log->debug( prefix_dbg()
                . " dbg torrent_top_lvl=$torrent_top_dbg is_multi=$is_multi_dbg"
        );

        # ---- Munge savepath + decide root rename (if hit path is known) ----
        if ( $hit_path ) {
          my $rename_dbg;
          my $munged_save_dbg;

          my $disk_file_dir_dbg   = dirname( $hit_path );
          my $disk_parent_dir_dbg = dirname( $disk_file_dir_dbg );
          my $disk_root_dbg       = basename( $disk_file_dir_dbg );

          ( $munged_save_dbg, $rename_dbg ) =
              munge_savepath_and_root_rename(
                                              rec      => $rec,
                                              hit_path => $hit_path,
                                              savepath => $savepath, );

          $app->log->debug(   prefix_dbg()
                            . "\n\tdbg disk_file_dir=$disk_file_dir_dbg"
                            . "\n\tdbg disk_parent_dir=$disk_parent_dir_dbg"
                            . "\n\tdbg disk_root_name=$disk_root_dbg"
                            . "\n\tdbg savepath_before=$savepath"
                            . "\n\tdbg savepath_after="
                            . ( $munged_save_dbg || '(empty)' ) );

          if ( ref( $rename_dbg ) eq 'HASH' ) {
            $app->log->debug( prefix_dbg()
                      . " dbg rename_intent: "
                      . ( $rename_dbg->{torrent_top_lvl} // '(undef)' ) . " -> "
                      . ( $rename_dbg->{drivespace_top_lvl} // '(undef)' ) );

            $savepath                 = $munged_save_dbg if $munged_save_dbg;
            $pending_root_rename_data = $rename_dbg;

            if ( ref( $pending_root_rename_data ) eq 'HASH' ) {
              my $new_root = $pending_root_rename_data->{drivespace_top_lvl}
                  // '';
              if ( length $new_root ) {
                QBTL::Store::store_put_local_override(
                         $app, $ih,
                         {
                          root_override            => $new_root,
                          pending_root_rename_data => $pending_root_rename_data,
                         } );
              }
            }
          }
          else {
            $app->log->debug( prefix_dbg() . " dbg rename_intent: (none)" );
          }
        }

        # ---- payload mount check on savepath ----
        my $v_payload = QBTL::Utils::vol_from_path( $savepath );
        if ( length( $v_payload ) && !$mounted->{$v_payload} ) {
          my $err =
              "Payload volume not mounted: $v_payload (savepath=$savepath)";
          _add_one_mark_fail( $c->app, $ih, $err );

          classify_triage(
                           $c->app,
                           {
                            ih          => $ih,
                            key         => 'missing_mount_payload',
                            path        => '/qbt/add_one',
                            err         => $err,
                            source_path => $source_path,
                            savepath    => $savepath,
                           },
                           'missing_mount' );

          _add_one_remove_ih( $c->app, $ih );
          die $err;
        }

        $app->log->debug(
                  prefix_dbg()
                . " savepath_final: $savepath "
                . (
              ref( $pending_root_rename_data ) eq 'HASH'
              ? " rename_root: $pending_root_rename_data->{torrent_top_lvl} -> "
                  . $pending_root_rename_data->{drivespace_top_lvl}
              : "" ) );

        # ---- Add torrent to qBittorrent now ----
        $add = $qbt->api_torrents_add( $source_path, $savepath );
        die "add returned non-HASH" unless ref( $add ) eq 'HASH';

        1;
      };

      unless ( $ok ) {
        my $err = "$@";
        chomp $err;

        $app->log->error( "DEBUG: eval failed in add_one: $err" );

        _add_one_mark_fail( $c->app, $ih, $err );
        classify_triage(
                         $c->app,
                         {
                          ih          => $ih,
                          key         => 'add_one_eval',
                          path        => '/qbt/add_one',
                          err         => $err,
                          source_path => $source_path,
                          savepath    => ( $savepath || '' ),
                         },
                         'add_one_failed', );

        $processed->{$ih} = {
                             status      => 'failed',
                             ts          => time,
                             last_error  => $err,
                             source_path => $source_path,
                             savepath    => ( $savepath || '' ),};

        _add_one_remove_ih( $c->app, $ih );
        my $st = _add_one_state( $c->app );
        $app->log->debug( prefix_dbg()
                  . "  [add_one] after remove ih: $ih idx: $st->{idx} queue_n: "
                  . scalar( @{$st->{queue} || []} ) );

        return $fail->( $err, ih => $ih, source_path => $source_path );
      }

   # If savepath went missing because we early-returned redirect (NO_HITS path),
   # we already redirected. But belt-and-suspenders:
      unless ( $savepath ) {
        return $c->redirect_to( $return_to );
      }

      my $body = $add->{body} // '';
      $app->log->debug( prefix_dbg()
        . " add_one: ok: $add->{ok} code=$add->{code} body: $body savepath: $savepath"
      );

      unless ( $add->{ok} ) {
        my $err = "Add failed: HTTP $add->{code} $body";
        _add_one_mark_fail( $c->app, $ih, $err );

        $processed->{$ih} = {
                             status      => 'failed',
                             ts          => time,
                             last_error  => $err,
                             source_path => $source_path,
                             savepath    => ( $savepath || '' ),};

        _add_one_remove_ih( $c->app, $ih );
        my $st = _add_one_state( $c->app );
        $app->log->debug( prefix_dbg()
                   . " [add_one] after remove ih: $ih idx: $st->{idx} queue_n: "
                   . scalar( @{$st->{queue} || []} ) );

        return $fail->( $err, ih => $ih, source_path => $source_path );
      }

      if ( $body =~ /fails/i ) {
        my $err = "qBittorrent refused add: $body";
        _add_one_mark_fail( $c->app, $ih, $err );

        $processed->{$ih} = {
                             status      => 'failed',
                             ts          => time,
                             last_error  => $err,
                             source_path => $source_path,
                             savepath    => ( $savepath || '' ),};

        _add_one_remove_ih( $c->app, $ih );
        my $st = _add_one_state( $c->app );
        $app->log->debug( prefix_dbg()
                   . " [add_one] after remove ih: $ih idx: $st->{idx} queue_n: "
                   . scalar( @{$st->{queue} || []} ) );

        return $fail->( $err, ih => $ih, source_path => $source_path );
      }

      # ---------- hand off to Queue.pm ----------
      my $rec = $local_by_ih->{$ih};
      if (    ref( $rec ) eq 'HASH'
           && ref( $rec->{pending_root_rename_data} ) eq 'HASH' )
      {
        $pending_root_rename_data = {%{$rec->{pending_root_rename_data}}};
      }

      my $rr_dbg = '(none)';
      if ( ref( $pending_root_rename_data ) eq 'HASH' ) {
        $rr_dbg =
              ( $pending_root_rename_data->{torrent_top_lvl} // '?' ) . " -> "
            . ( $pending_root_rename_data->{drivespace_top_lvl} // '?' );
      }

      $app->log->debug( prefix_dbg() . " rename_intent: $rr_dbg\n" );

      QBTL::Queue->enqueue_add_one(
                          $c->app,
                          ih                       => $ih,
                          source_path              => $source_path,
                          savepath                 => ( $savepath || '' ),
                          pending_root_rename_data => $pending_root_rename_data,
      );

      $processed->{$ih} = {
                           status      => 'pending',
                           ts          => time,
                           last_error  => 'queued to QBTL::Queue',
                           source_path => $source_path,
                           savepath    => ( $savepath || '' ),};

      _add_one_remove_ih( $c->app, $ih );
      my $st = _add_one_state( $c->app );

      $app->log->debug( prefix_dbg()
                   . " [add_one] after remove ih: $ih idx: $st->{idx} queue_n: "
                   . scalar( @{$st->{queue} || []} )
                   . "\n\n" );

      $c->app->defaults->{qbt_snap_ts} = 0;

      $app->log->debug(
               prefix_dbg() . " add_one queued ih: $ih return_to: $return_to" );

      return $c->redirect_to( $return_to );
    } );

  $r->post(
    '/api/add_one_by_ih' => sub {
      my $c = shift;

      my $ih = $c->param( 'ih' ) // '';
      return $c->render( json => {ok => 0, err => 'bad ih'}, status => 400 )
          unless $ih =~ /^[0-9a-f]{40}$/;

      # Canonical in-memory cache (preferred)
      my $by_ih = $c->app->defaults->{localcache}{by_ih};
      $by_ih = {} if ref( $by_ih ) ne 'HASH';

      my $rec = $by_ih->{$ih};

      # Fallback: load from disk cache if not in memory
      if ( ref( $rec ) ne 'HASH' ) {
        my ( $local_by_ih ) =
            QBTL::LocalCache::get_local_by_ih(
                           $c->app,
                           root_dir => ( $c->app->defaults->{root_dir} || '.' ),
                           opts_local => {torrent_dir => "/"}, );
        $local_by_ih = {} if ref( $local_by_ih ) ne 'HASH';
        $rec         = $local_by_ih->{$ih};
      }

      return
          $c->render(
        json => {ok => 0, err => "No cache record for ih=$ih (rebuild cache?)"},
        status => 404,
          ) unless ref( $rec ) eq 'HASH';

      my $source_path = $rec->{source_path} // '';
      return
          $c->render(
          json   => {ok => 0, err => "missing source_path in cache for ih=$ih"},
          status => 422,
          ) unless length $source_path;

      my $ok = eval {
        require QBTL::Queue;

# enqueue_add_one signature varies; use the one you already have.
# Common patterns:
#   QBTL::Queue->enqueue_add_one($c->app, ih => $ih, source_path => $source_path);
# or QBTL::Queue::enqueue_add_one($c->app, $ih, source_path => $source_path);
        QBTL::Queue->enqueue_add_one( $c->app, $ih,
                                      source_path => $source_path );
        1;
      };

      if ( !$ok ) {
        my $e = "$@";
        chomp $e;
        return $c->render( json => {ok => 0, err => $e}, status => 500 );
      }

      return
          $c->render(
                      json => {
                               ok          => 1,
                               ih          => $ih,
                               source_path => $source_path,
                               msg         => 'enqueued',
                      } );
    } );

  $r->get(
    '/qbt/hashnames' => sub {
      my $c = shift;

      $c->app->log->debug( prefix_dbg() . " ENTER /qbt/hashnames" );
      my $t0  = time;
      my $qbt = QBTL::QBT->new( {app => $c->app} );

      # pull fresh every time (for now)
      my $list = $qbt->api_torrents_info() || [];
      $list = [] if ref( $list ) ne 'ARRAY';

      my @rows;
      for my $t ( @$list ) {
        next unless ref( $t ) eq 'HASH';

####################
        $app->log->debug( prefix_dbg() . "\n" );
        for my $key ( keys $t->%* ) {
          $app->log->debug( prefix_dbg() . " $key" . " => " . $t->{$key} );
        }
###################

        my $ih   = $t->{hash} // '';
        my $name = $t->{name} // '';
        next unless $ih =~ /^[0-9a-f]{40}$/;
        next
            unless length( $name ) && $name eq $ih;    # only “ihname” torrents

        push @rows,
            {
             ih        => $ih,
             name      => $name,
             progress  => ( exists $t->{progress} ? $t->{progress} : '' ),
             save_path => ( $t->{save_path} // '' ),};
      }

      $c->stash(
                 mode   => 'all',
                 found  => scalar( @rows ),
                 sample => \@rows,
                 per    => scalar( @rows ), );

      $c->app->log->debug(
              prefix_dbg() . " EXIT /qbt/hashnames dt=" . ( time - $t0 ) . "s" )
          . " HASHNAMES stash keys="
          . join( ",", sort keys %{$c->stash} );
      my $qh = $c->app->defaults->{store}{qbt}{by_ih} || {};
      $c->app->log->debug(
               prefix_dbg() . " HASHNAMES qbt_store_n=" . scalar( keys %$qh ) );

      return $c->render( template => 'qbt_hashnames' );
    } );

  $r->get(
    '/qbt/no_payload' => sub {
      my $c = shift;

      my $h = $c->app->defaults->{classes}{NO_PAYLOAD};
      $h = {} if ref( $h ) ne 'HASH';

      # newest first
      my @keys =
          sort { ( $h->{$b}{ts} // 0 ) <=> ( $h->{$a}{ts} // 0 ) }
          keys %$h;

      my @rows = map {
        my $r = $h->{$_};
        $r = {} if ref( $r ) ne 'HASH';
        +{
          ih       => ( $r->{ih}       // $_ ),
          subclass => ( $r->{subclass} // '' ),
          ts       => ( $r->{ts}       // 0 ),
          why      => ( $r->{why}      // '' ),

          name        => ( $r->{name}        // '' ),
          bucket      => ( $r->{bucket}      // '' ),
          tracker     => ( $r->{tracker}     // '' ),
          source_path => ( $r->{source_path} // '' ),
          total_size  => ( $r->{total_size}  // 0 ),

          vol      => ( $r->{vol}      // '' ),
          hit_path => ( $r->{hit_path} // '' ),
          leaf     => ( $r->{leaf}     // '' ),}
      } @keys;

      # ---- bucket classifier (subclass wins; why is fallback) ----
      my $bucket_of = sub {
        my ( $r ) = @_;
        my $sub   = uc( $r->{subclass} // '' );
        my $why   = lc( $r->{why}      // '' );

        return 'INTERNAL'       if $sub eq 'INTERNAL';
        return 'NO_HITS'        if $sub eq 'NO_HITS';
        return 'VERIFY_FAILED'  if $sub eq 'VERIFY_FAILED';
        return 'BAD_META'       if $sub eq 'BAD_META';
        return 'VOLUME_MISSING' if $sub eq 'VOLUME_MISSING';

        # fallback by "why"
        return 'VOLUME_MISSING' if $why =~ /\bvolume\b|\bunmount/;
        return 'VERIFY_FAILED'  if $why =~ /\bverify\b|\broot_mismatch\b/;
        return 'BAD_META'
            if $why =~
            /\bbad rec\b|\bno files\b|\bno usable anchors\b|\bmetadata\b/;
        return 'INTERNAL'
            if $why =~
            /\binternal\b|\bunexpected\b|\bcaller\b|\bbug\b|\bunreachable\b/;

        # default: treat unknown as INTERNAL-ish (investigate)
        return 'INTERNAL';
      };

      # tier2: what you want to “subclass” under each tier1
      my $tier2_of = sub {
        my ( $r ) = @_;
        my $k = $r->{why} // '';
        $k =~ s/^\s+|\s+$//g;
        $k = '(none)' if $k eq '';
        return $k;
      };

      my %bucket;
      for my $r ( @rows ) {
        push @{$bucket{$bucket_of->( $r )}}, $r;
      }

      # priority order
      my @order = qw(
          INTERNAL
          NO_HITS
          VERIFY_FAILED
          BAD_META
          VOLUME_MISSING
      );

      my @sections;
      for my $k ( @order ) {
        my $list = $bucket{$k} || [];
        next unless @$list;

        # group into tier2 buckets
        my %g;
        for my $r ( @$list ) {
          push @{$g{$tier2_of->( $r )}}, $r;
        }

        # optional: sort tier2 groups by count desc, then name
        my @groups = map {
          +{
            key   => $_,
            count => scalar @{$g{$_}},
            rows  => $g{$_},             # keep if you want drill-down later
          }
        } sort { scalar( @{$g{$b}} ) <=> scalar( @{$g{$a}} ) || $a cmp $b }
            keys %g;

        push @sections,
            +{
              key    => $k,
              count  => scalar( @$list ),
              groups => \@groups,};
      }

      $c->stash( sections => \@sections );
      return $c->render( template => 'qbt_no_payload' );
    } );

  $r->get(    # Page_View
   '/torrents' => sub {
     my $c = shift;
     $c->stash( dev_mode => ( $opts->{dev_mode} ? 1 : 0 ) );

     my $mode = $c->param( 'mode' ) // '';
     $mode = ( $mode eq 'scroll' ) ? 'scroll' : 'paginate';

     my $per = int( $c->param( 'per' ) // 20 );
     $per = 20  if $per < 20;
     $per = 500 if $per > 500;

     my $page = int( $c->param( 'page' ) // 1 );
     $page = 1 if $page < 1;

     my $q = $c->param( 'q' ) // '';
     $q =~ s/\s+//g if $q =~ /[0-9a-f]{20}/i;    # only compact when hash-ish

     my $has_q = length( $q ) ? 1 : 0;

     # show=missing (default) | all
     my $show = $c->param( 'show' ) // 'missing';
     $show = ( $show eq 'all' ) ? 'all' : 'missing';

# ---------- Runtime overlay: things we already touched this server run ----------
     $c->app->defaults->{runtime} ||= {};
     $c->app->defaults->{runtime}{processed} ||= {};
     my $processed = $c->app->defaults->{runtime}{processed};

# ---------- Classes overlay (NO_HITS should disappear from Page_View list) ----------
     $c->app->defaults->{classes} ||= {};
     $c->app->defaults->{classes}{NO_PAYLOAD} ||= {};
     my $no_payload = $c->app->defaults->{classes}{NO_PAYLOAD};
     $no_payload = {} if ref( $no_payload ) ne 'HASH';

     # ---------- Load local cache ----------
     my ( $local_by_ih, $mtime, $src ) =
         QBTL::LocalCache::get_local_by_ih(
                                       $app,
                                       root_dir => ( $opts->{root_dir} || '.' ),
                                       opts_local => {torrent_dir => "/"}, );
     $local_by_ih = {} if ref( $local_by_ih ) ne 'HASH';

     $c->stash( local_cache_mtime => ( $mtime || 0 ) );
     $c->stash( local_cache_src   => ( $src   || '' ) );

     # ---------- QBT snapshot (cached in memory) ----------
     my $snap_ttl = 30;
     my $snap_ts  = $c->app->defaults->{qbt_snap_ts} || 0;
     if ( !$c->app->defaults->{qbt_by_ih} || ( time - $snap_ts ) > $snap_ttl ) {
       my $qbt_by_ih = {};
       eval {
         my $qbt = QBTL::QBT->new( {app => $c->app} );
         $qbt_by_ih = $qbt->api_torrents_infohash_map;
         store_put_qbt_snapshot( $c->app, $qbt_by_ih );
         $qbt_by_ih = {} if ref( $qbt_by_ih ) ne 'HASH';
         1;
           }
           or do {
         $qbt_by_ih = $c->app->defaults->{qbt_by_ih} || {};
           };
       $c->app->defaults->{qbt_by_ih}   = $qbt_by_ih;
       $c->app->defaults->{qbt_snap_ts} = time;
     }
     my $qbt_by_ih = $c->app->defaults->{qbt_by_ih} || {};
     $qbt_by_ih = {} if ref( $qbt_by_ih ) ne 'HASH';

     # ---------- Build rows (default: only "missing from qbt") ----------
     my $local_total = 0;
     my $in_qbt      = 0;

     my @rows;
     my $mounted = QBTL::Utils::mounted_vols_map();
     $mounted = {} if ref( $mounted ) ne 'HASH';

     my $ignore_all = $c->session( 'ignore_unmounted' ) ? 1 : 0;
     for my $ih ( keys %$local_by_ih ) {

#        $c->app->log->debug(   prefix_dbg()
#                             . "IH_KEY='$ih' has_q=$has_q show=$show processed="
#                             . ( exists $processed->{$ih} ? 1 : 0 )
#                             . " no_payload="
#                             . ( exists $no_payload->{$ih} ? 1 : 0 ) );

       next unless defined $ih && $ih =~ /^[0-9a-f]{40}$/;
       next if ( !$has_q && exists $processed->{$ih} );
       next if exists $no_payload->{$ih};

       my $rec = $local_by_ih->{$ih};
       next unless ref( $rec ) eq 'HASH';

       $local_total++;

       my $exists_in_qbt = exists $qbt_by_ih->{$ih} ? 1 : 0;
       $in_qbt++ if $exists_in_qbt;
       next      if ( !$has_q && ( $show eq 'missing' ) && $exists_in_qbt );

       my $source_path = $rec->{source_path} // '';
       if ( $ignore_all && length $source_path ) {
         my $v = QBTL::Utils::vol_from_path( $source_path );
         if ( length( $v ) && !$mounted->{$v} ) {
           next;    # <-- cull unmounted torrent volume from the user's face
         }
       }
       my $name  = $rec->{name} // '';
       my $files = $rec->{files};

       my $total_size = $rec->{total_size};
       $total_size = 0 if !defined $total_size;

       if ( !$name && $source_path ) {
         ( $name ) = $source_path =~ m{([^/]+)\z};
         $name ||= '';
       }

       push @rows,
           {
            ih          => $ih,
            name        => $name,
            total_size  => ( $total_size || 0 ),
            source_path => $source_path,
            files_count => ( ref( $files ) eq 'ARRAY' ) ? scalar( @$files ) : 0,
            in_qbt      => $exists_in_qbt,};
     }

     # ---------- Search filter ----------
     if ( length $q ) {
       my $re = qr/\Q$q\E/i;
       @rows = grep {
                ( ( $_->{ih} // '' ) =~ $re )
             || ( ( $_->{name}        // '' ) =~ $re )
             || ( ( $_->{source_path} // '' ) =~ $re )
       } @rows;
     }

     # ---------- Sort ----------
     my $sort = $c->param( 'sort' ) // 'name';
     $sort = ( $sort eq 'size' ) ? 'size' : 'name';

     # Safety valve: size+scroll forces paginate
     if ( $sort eq 'size' && $mode eq 'scroll' ) {
       $mode = 'paginate';
     }

     if ( $sort eq 'size' ) {
       @rows = sort { ( $b->{total_size} // 0 ) <=> ( $a->{total_size} // 0 ) }
           @rows;
     }
     else {
       @rows = sort { ( $a->{name} // '' ) cmp( $b->{name} // '' ) } @rows;
     }

     my $found = scalar( @rows );

     my $autoback = $c->param( 'autoback' ) // '';
     if ( $autoback && $found == 0 ) {

       # Keep view knobs, but drop q + autoback + page
       my $mode = $c->param( 'mode' ) // '';
       $mode = ( $mode eq 'scroll' ) ? 'scroll' : 'paginate';

       my $show = $c->param( 'show' ) // 'missing';
       $show = ( $show eq 'all' ) ? 'all' : 'missing';

       my $sort = $c->param( 'sort' ) // 'name';
       $sort = ( $sort eq 'size' ) ? 'size' : 'name';

       my $per = int( $c->param( 'per' ) // 20 );
       $per = 20  if $per < 20;
       $per = 500 if $per > 500;

       my $fallback =
           "/torrents?mode=$mode&show=$show&sort=$sort&per=$per&page=1";

       return $c->redirect_to( $fallback );
     }

     my ( $pages, $start_n, $end_n ) = ( 1, 0, 0 );
     my @sample = @rows;

     if ( $mode eq 'paginate' ) {
       $pages = int( ( $found + $per - 1 ) / $per ) || 1;
       $page  = $pages if $page > $pages;

       my $start_idx = ( $page - 1 ) * $per;
       my $end_idx   = $start_idx + $per - 1;
       $end_idx = $found - 1 if $end_idx > $found - 1;

       @sample  = ( $found ? @rows[ $start_idx .. $end_idx ] : () );
       $start_n = $found ? ( $start_idx + 1 ) : 0;
       $end_n   = $found ? ( $end_idx + 1 )   : 0;
     }
     else {
       $pages   = 1;
       $page    = 1;
       $start_n = $found ? 1      : 0;
       $end_n   = $found ? $found : 0;
     }

     my $human = sub {
       my ( $n ) = @_;
       $n ||= 0;
       return QBTL::Utils::human_bytes( $n );
     };

     $c->stash(
       mode    => $mode,
       per     => $per,
       page    => $page,
       pages   => $pages,
       q       => $q,
       show    => $show,
       found   => $found,
       start_n => $start_n,
       end_n   => $end_n,
       sample  => \@sample,
       human   => $human,

       local_total    => $local_total,
       in_qbt_count   => $in_qbt,
       missing_count  => ( $local_total - $in_qbt ),
       qbt_snap_age_s =>
           ( time - ( $c->app->defaults->{qbt_snap_ts} || time ) ), );

     return $c->render( template => 'torrents' );
   } );

  $r->get(
    '/qbt/triage' => sub {
      my $c = shift;
      $c->app->defaults->{classes} ||= {};

      if ( ref( $c->app->defaults->{classes}{TRIAGE} ) ne 'HASH' ) {
        $c->app->defaults->{classes}{TRIAGE} = {};
      }

      my $h = $c->app->defaults->{classes}{TRIAGE};
      my @ids =
          sort { ( $h->{$b}{ts} // 0 ) <=> ( $h->{$a}{ts} // 0 ) } keys %$h;
      my @rows = map {
        my $id  = $_;
        my $rec = $h->{$id};
        $rec = {} if ref( $rec ) ne 'HASH';
        +{id => $id, %$rec}
      } @ids;

      my $newest_id = $ids[0];
      my $newest_ih =
            ( $newest_id && ref( $h->{$newest_id} ) eq 'HASH' )
          ? ( $h->{$newest_id}{ih} // '' )
          : '';

      $c->app->log->debug(
                      prefix_dbg()
                    . " TRIAGE: "
                    . (
                  scalar( @ids )
                  ? " keys: " . scalar( @ids ) . " newest_ih: " . ( $newest_ih )
                  : "no records to triage" ) );

      $c->stash( found => scalar( @rows ),
                 rows  => \@rows, );
      return $c->render( template => 'qbt_triage' );
    } );

  $r->get(
    '/qbt/view' => sub {
      my $c = shift;

      my $ih        = lc( $c->param( 'ih' ) // '' );
      my $return_to = $c->param( 'return_to' ) // '/torrents';

      # sanitize return_to
      $return_to = '/torrents'
          if $return_to !~ m{\A/}
          || $return_to =~ m{://}
          || $return_to =~ m{[\r\n]};

      unless ( $ih =~ /^[0-9a-f]{40}$/ ) {
        $c->flash( notice => "Bad ih" );
        return $c->redirect_to( $return_to );
      }

      # Prefer in-memory canonical store
      my $by_ih = $c->app->defaults->{localcache}{by_ih};
      $by_ih = {} if ref( $by_ih ) ne 'HASH';

      my $rec = $by_ih->{$ih};

      # Fallback: load cache if store not yet populated
      if ( ref( $rec ) ne 'HASH' ) {
        my ( $local_by_ih ) =
            QBTL::LocalCache::get_local_by_ih(
                           $c->app,
                           root_dir => ( $c->app->defaults->{root_dir} || '.' ),
                           opts_local => {torrent_dir => "/"}, );
        $local_by_ih = {} if ref( $local_by_ih ) ne 'HASH';
        $rec         = $local_by_ih->{$ih};
      }

      unless ( ref( $rec ) eq 'HASH' ) {
        $c->flash( notice => "No cache record for ih=$ih (rebuild cache?)" );
        return $c->redirect_to( $return_to );
      }

      $c->stash(
                 ih        => $ih,
                 rec       => $rec,
                 return_to => $return_to, );

      return $c->render( template => 'qbt_view_meta' );
    } );

  $r->get(
    '/api/view_meta' => sub {
      my $c  = shift;
      my $ih = lc( $c->param( 'ih' ) // '' );

      return $c->render( json => {ok => 0, err => 'bad ih'}, status => 400 )
          unless $ih =~ /^[0-9a-f]{40}$/;

      my $by_ih = $c->app->defaults->{localcache}{by_ih};
      $by_ih = {} if ref( $by_ih ) ne 'HASH';
      my $rec = $by_ih->{$ih};

      if ( ref( $rec ) ne 'HASH' ) {
        my ( $local_by_ih ) =
            QBTL::LocalCache::get_local_by_ih(
                           $c->app,
                           root_dir => ( $c->app->defaults->{root_dir} || '.' ),
                           opts_local => {torrent_dir => "/"}, );
        $local_by_ih = {} if ref( $local_by_ih ) ne 'HASH';
        $rec         = $local_by_ih->{$ih};
      }

      return $c->render( json => {ok => 0, err => 'not found'}, status => 404 )
          unless ref( $rec ) eq 'HASH';

      my $name = $rec->{name} // '';
      $name =~ s/^\s+|\s+$//g;

      if ( !$name ) {
        my $sp = $rec->{source_path} // '';
        if ( length $sp ) {
          ( $name ) = $sp =~ m{([^/]+)\z};
          $name ||= '';
          $name =~ s/\.torrent\z//i;
        }
      }

      my $files = $rec->{files};
      $files = [] if ref( $files ) ne 'ARRAY';
      my @paths = map {
        ( ref( $_ ) eq 'HASH' && defined $_->{path} )
            ? $_->{path}
            : ()
      } @$files;

      return
          $c->render(
                      json => {
                               ok          => 1,
                               ih          => $ih,
                               name        => $name,
                               source_path => ( $rec->{source_path} // '' ),
                               files       => \@paths,
                      } );
    } );

  # ---------- Idle observer tick ----------
  my $tick_seconds = 5;
  QBTL::Queue->start( $app, opts => $opts, tick_seconds => $tick_seconds );

  return $app;
}

sub _add_one_advance {
  my ( $app ) = @_;
  my $st      = _add_one_state( $app );
  my $n       = scalar( @{$st->{queue} || []} );
  return undef if !$n;

  $st->{idx}++;
  $st->{idx} = 0 if $st->{idx} >= $n;

  return $st->{queue}[ $st->{idx} ];
}

sub _add_one_build_queue {
  my ( %args )    = @_;
  my $app         = $args{app} or die "missing app";
  my $local_by_ih = $args{local_by_ih} || {};
  my $qbt_by_ih   = $args{qbt_by_ih}   || {};
  my $cache_mtime = $args{cache_mtime} || 0;

  my $st = _add_one_state( $app );

  my @q;
  for my $ih ( keys %$local_by_ih ) {
    next unless defined $ih && $ih =~ /^[0-9a-f]{40}$/;
    next if exists $qbt_by_ih->{$ih};
    push @q, $ih;
  }

  # Optional: pseudo-randomize without relying on ih order
  @q = sort { rand() <=> rand() } @q;

  $st->{queue}       = \@q;
  $st->{idx}         = 0;
  $st->{cache_mtime} = $cache_mtime;

  return;
}

sub _add_one_mark_fail {
  my ( $app, $ih, $err ) = @_;
  $ih = ( $ih // '' );
  return unless $ih =~ /^[0-9a-f]{40}$/;

  my $st = _add_one_state( $app );
  $st->{meta}{$ih} ||= {};
  $st->{meta}{$ih}{failed_once} = 1;
  $st->{meta}{$ih}{last_error}  = ( defined $err ? $err : '' );
  $st->{meta}{$ih}{ts}          = time;

  return;
}

sub _add_one_remove_ih {
  my ( $app, $ih ) = @_;
  $ih = ( $ih // '' );
  return unless $ih =~ /^[0-9a-f]{40}$/;

  my $st  = _add_one_state( $app );
  my $q   = $st->{queue} || [];
  my $idx = $st->{idx}   || 0;

  for ( my $i = 0 ; $i < @$q ; $i++ ) {
    next unless defined $q->[$i];
    next unless $q->[$i] eq $ih;

    splice( @$q, $i, 1 );

    # if we removed something before the current idx, shift idx left
    if ( $i < $idx ) {
      $idx--;
    }

    last;
  }

  $idx = 0       if $idx < 0;
  $idx = 0       if !@$q;
  $idx = @$q - 1 if @$q && $idx > @$q - 1;

  $st->{idx}   = $idx;
  $st->{queue} = $q;

  return 1;
}

sub _add_one_state {
  my ( $app ) = @_;
  $app->defaults->{add_one} ||= {
    queue => [],  # arrayref of infohashes
    idx   => 0,   # current cursor
    meta  => {},  # { ih => { failed_once => 1, last_error => "...", ts => epoch
    pending => {}
    , # { ih => { tries => N, first_ts => epoch, last_ts => epoch, last_error => "..."}
    cache_mtime => 0,    # mtime used when queue was built
  };
  return $app->defaults->{add_one};
}

sub _fmt_ts {
  my ( $epoch ) = @_;
  return '' unless $epoch;
  my @lt = localtime( $epoch );
  return
      sprintf( "%04d-%02d-%02d %02d:%02d:%02d",
               $lt[5] + 1900,
               $lt[4] + 1,
               $lt[3], $lt[2], $lt[1], $lt[0] );
}

sub _fmt_savepath_debug {
  my ( $dbg ) = @_;
  return '' unless $dbg && ref( $dbg ) eq 'ARRAY' && @$dbg;

  my @out;

  push @out, "";
  push @out, "SavePath debug (most recent attempts):";

  for my $a ( @$dbg ) {
    next unless ref( $a ) eq 'HASH';

    push @out, "";
    push @out, "-" x 50;
    push @out, "Anchor:";
    push @out, sprintf( "  rel:      %s", $a->{anchor_rel} // '' );
    push @out, sprintf( "  leaf:     %s", $a->{leaf}       // '' );
    push @out, sprintf( "  want_len: %s", $a->{want_len}   // '' );

    push @out, "";
    push @out, "Spotlight:";
    push @out, "  size-locked hit: " . ( $a->{hit_size} || '(none)' );
    push @out, "  name-only hit:  " .  ( $a->{hit_any}  || '(none)' );

    push @out, "";
    push @out, "Derived savepath:";
    push @out, "  " . ( $a->{savepath} || '(none)' );

    push @out, "";
    push @out, "Verification:";

    if (    $a->{verify}
         && ref( $a->{verify} ) eq 'ARRAY'
         && @{$a->{verify}} )
    {
      for my $v ( @{$a->{verify}} ) {
        next unless ref( $v ) eq 'HASH';
        my $flag = $v->{exists} ? '[OK]  ' : '[MISS]';
        push @out, "  $flag $v->{full}";
      }
    }
    else {
      push @out, "  (not attempted)";
    }

    push @out, "";
    push @out, "Result:";
    push @out, "  " . ( $a->{accept} ? 'ACCEPTED' : "rejected: $a->{reject}" );
  }

  push @out, "-" x 50;
  push @out, "";

  return join( "\n", @out );
}

sub _infohash_from_torrent_file {
  my ( $app, $path ) = @_;
  require Bencode;

  my $raw;
  my $used = $path;

  # macOS filenames can be NFC or NFD; try both (plus original)
  for my $try ( $path, NFC( $path ), NFD( $path ) ) {
    next unless defined $try && length $try;

    $app->log->debug( prefix_dbg() . " TRY: $try\n" );

    if ( open my $fh, '<:raw', $try ) {
      local $/;
      $raw  = <$fh>;
      $used = $try;
      last;
    }
  }
  die "open($path): $!" unless defined $raw;

  my $t = Bencode::bdecode( $raw );
  die "not a torrent (no info dict)"
      unless ref( $t ) eq 'HASH' && ref( $t->{info} ) eq 'HASH';

  my $info_bencoded = Bencode::bencode( $t->{info} );
  return sha1_hex( $info_bencoded );
}

sub _is_multi_file_torrent {
  my ( $rec ) = @_;
  return 0 unless ref( $rec ) eq 'HASH';
  my $files = $rec->{files};
  return 0 unless ref( $files ) eq 'ARRAY' && @$files;

  # multi-file torrents usually have "root/child" paths (or >1 file)
  return 1 if @$files > 1;

  my $p = $files->[0]{path} // '';
  return ( $p =~ m{/} ) ? 1 : 0;
}

sub _open_raw_try_nf {
  my ( $path ) = @_;
  for my $try ( $path, NFC( $path ), NFD( $path ) ) {
    next unless defined $try && length $try;
    if ( open my $fh, '<:raw', $try ) {
      return ( $fh, $try );
    }
  }
  die "open($path): $!";    # keep message simple; can expand if you want
}

1;

