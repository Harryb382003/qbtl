package QBTL::Web;
use common::sense;

use Digest::SHA qw(sha1_hex);
use File::Spec;
use File::Basename qw(dirname basename);
use FindBin        qw($Bin);
use Mojolicious;
use Mojo::JSON qw(true);

use QBTL::LocalCache;
use QBTL::QBT;
use QBTL::Queue;
use QBTL::SavePath qw(
    munge_savepath_and_root_rename
    torrent_top_lvl_from_rec
);
use QBTL::Utils qw(
    start_timer
    stop_timer
    short_ih
    prefix_dbg
);
use QBTL::Logger;

QBTL::Logger::enable_disk_logging( path => 'qbtl.log', level => 'debug' );

BEGIN {
  my $repo_root = File::Spec->catdir( $Bin, '..' );    # bin/.. => repo root
  my $log_path =
      File::Spec->catfile( $repo_root, 'qbtl.log' );    # repo_root/qbtl.log
  QBTL::Logger::enable_disk_logging( path => $log_path, level => 'debug' );
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

  # Optional: pseudo-randomize without relying on hash order
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

sub _add_one_remove_hash {
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
  my ( $path ) = @_;
  require Bencode;
  my $raw = do {
    open my $fh, '<:raw', $path or die "open($path): $!";
    local $/;
    <$fh>;
  };
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

sub app {
  my ( $opts ) = @_;
  $opts ||= {};
  my $app = Mojolicious->new;
  QBTL::Logger::set_log_file( $opts->{log_file} || 'qbtl.log' );
  $app->log->level( 'debug' );
  $app->log->debug( basename( __FILE__ ) . ":" . __LINE__ . " pid=$$" );
  $app->defaults->{dev_mode} = ( $opts->{dev_mode} ? 1 : 0 );

  my $root = $opts->{root_dir} || '.';
  $app->defaults->{root_dir} = $root;

  # Defaults for cache metadata
  $app->defaults->{local_cache_mtime} = 0;
  $app->defaults->{local_cache_src}   = '';

  push @{$app->renderer->paths}, File::Spec->catdir( $root, 'templates' );
  push @{$app->static->paths},   File::Spec->catdir( $root, 'ui' );

  # Keep cache stamp fresh (prefer .stor else .json)
  $app->hook(
    before_dispatch => sub {
      my $c = shift;

      # --- PROVE REQUESTS ARE HITTING MOJO ---
      my $m = $c->req->method                // '';
      my $p = $c->req->url->path->to_string  // '';
      my $q = $c->req->url->query->to_string // '';
      $c->app->log->debug(   basename( __FILE__ ) . ":"
                           . __LINE__
                           . " REQ $m $p"
                           . ( length( $q ) ? "?$q" : "" ) );
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
    $app->log->debug(
       basename( __FILE__ ) . ":" . __LINE__ . " Developer routes registered" );
  }

  $r->get(
    '/' => sub {
      my $c = shift;
      $c->stash( dev_mode => ( $opts->{dev_mode} ? 1 : 0 ) );

      # ---------- Load local cache ----------
      my ( $local_by_ih, $mtime, $src ) =
          QBTL::LocalCache::get_local_by_ih(
                                       root_dir => ( $opts->{root_dir} || '.' ),
                                       opts_local => {torrent_dir => "/"}, );
      $local_by_ih = {} if ref( $local_by_ih ) ne 'HASH';
      $c->stash( local_cache_mtime => ( $mtime || 0 ) );
      $c->stash( local_cache_src   => ( $src   || '' ) );

      my $local_count = scalar( keys %$local_by_ih );

      # ---------- Load qBittorrent state ----------
      my ( $qbt_by_ih, $qbt_list );
      my $qbt_err = '';
      eval {
        my $qbt = QBTL::QBT->new( {} );
        $qbt_by_ih = $qbt->api_torrents_infohash_map();
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

      my $name_is_hash = 0;
      my $repairable   = 0;

      for my $t ( @$qbt_list ) {
        next if ref( $t ) ne 'HASH';
        my $name = $t->{name} // '';
        next unless $name =~ /^[0-9a-fA-F]{40}$/;
        $name_is_hash++;

        my $ih = lc( $t->{hash} // '' );
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
             hash          => $ih,
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
                    qbt_name_is_hash => $name_is_hash,
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
    '/opts' => sub {
      my $c = shift;
      $c->render( json => {dev_mode => ( $opts->{dev_mode} ? 1 : 0 )} );
    } );

  $r->get(
    '/health' => sub {
      my $c = shift;
      $c->render( json => {ok => true, app => 'qbtl'} );
    } );

  $r->get(
    '/legacy_smoke' => sub {
      my $c   = shift;
      my $out = {};
      eval {
        require Utils;
        $out->{os} = Utils::test_OS();
        1;
          }
          or do {
        $out->{error} = "$@";
          };
      $c->render( json => $out );
    } );

  $r->get(
    '/parse_smoke' => sub {
      my $c   = shift;
      my $res = eval { QBTL::Parse::run( all_torrents => [], opts => {} ) };
      return $c->render( json => {error => "$@"} ) if $@;
      $c->render( json => {ok => Mojo::JSON->true} );
    } );

  $r->get(
    '/qbt' => sub {
      my $c = shift;
      $c->reply->static( 'qbt.html' );
    } );

  $r->get(
    '/qbt/add_one' => sub {
      my $c = shift;
      $c->app->log->debug(   prefix_dbg()
                           . "  add_one ctx method="
                           . $c->req->method
                           . " path="
                           . $c->req->url->path . " ref="
                           . ( $c->req->headers->referrer // '(none)' )
                           . " return_to="
                           . ( $c->param( 'return_to' ) // '(none)' )
                           . " hash="
                           . ( $c->param( 'hash' ) // '(none)' ) );
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
          QBTL::LocalCache::get_local_by_ih(
                                       root_dir => ( $opts->{root_dir} || '.' ),
                                       opts_local => {torrent_dir => "/"}, );
      $local_by_ih = {} if ref( $local_by_ih ) ne 'HASH';

      # Pull qbt snapshot ONLY when we need to (queue build / rebuild)
      my $qbt       = QBTL::QBT->new( {} );
      my $qbt_by_ih = $qbt->api_torrents_infohash_map;
      $qbt_by_ih = {} if ref( $qbt_by_ih ) ne 'HASH';

      my $st = _add_one_state( $c->app );

      # Rebuild queue if empty OR cache has changed since queue build
      unless ( @{$st->{queue} || []}
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

      # Optional explicit hash (?hash=...)
      my $want_hash = $c->param( 'hash' ) // '';
      $want_hash =~ s/\s+//g;
      $want_hash = '' unless $want_hash =~ /^[0-9a-f]{40}$/;

      my $pick_hash;

      if ( $want_hash ) {
        $pick_hash = $want_hash;

        unless ( exists $local_by_ih->{$pick_hash} ) {
          return
              $c->render(
                 template => 'qbt_add_one',
                 error => "Requested hash not found in local cache: $want_hash",
                 return_to => $return_to, );
        }

        if ( exists $qbt_by_ih->{$pick_hash} ) {
          return
              $c->render(
            template => 'qbt_add_one',
            error    =>
"Requested hash already exists in qBittorrent: \n\t$pick_hash\n\t$want_hash",
            return_to => $return_to, );
        }
      }
      else {
        $pick_hash = $st->{queue}[ $st->{idx} ] if @{$st->{queue} || []};
      }

      return
          $c->render(
                      template  => 'qbt_add_one',
                      return_to => $return_to,
          ) unless $pick_hash;

      my $rec = $local_by_ih->{$pick_hash};
      my $source_path =
          ( ref( $rec ) eq 'HASH' ) ? ( $rec->{source_path} // '' ) : '';

      if ( !$source_path ) {
        return
            $c->render(
                        template => 'qbt_add_one',
                        error => "Picked $pick_hash but no source_path found",
                        return_to => $return_to, );
      }

      my $meta = _add_one_state( $c->app )->{meta}{$pick_hash} || {};

      $c->stash(
                 picked => {
                            hash        => $pick_hash,
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

      my $hash = $c->param( 'hash' ) // '';
      $hash =~ s/\s+//g;

      my $source_path = $c->param( 'source_path' ) // '';

      return
          $c->render(
                      template => 'qbt_add_one',
                      error    => "bad hash",
                      picked   => {
                                 hash        => $hash,
                                 source_path => $source_path
                      },
          ) unless $hash =~ /^[0-9a-f]{40}$/;

      my $confirm = $c->param( 'confirm' ) // '';
      my $retry   = $c->param( 'retry' )   // 0;    # hook only (unused for now)

      unless ( $confirm ) {
        return
            $c->render(
                        template => 'qbt_add_one',
                        error    => "confirm required",
                        picked   => {
                                   hash        => $hash,
                                   source_path => $source_path,
                        }, );
      }

      unless ( $source_path ) {
        return
            $c->render(
                        template => 'qbt_add_one',
                        error    => "missing source_path",
                        picked   => {
                                   hash        => $hash,
                                   source_path => $source_path,
                        }, );
      }

      # ---------- Runtime overlay for Page View culling ----------
      $c->app->defaults->{runtime} ||= {};
      $c->app->defaults->{runtime}{processed} ||= {};
      my $processed = $c->app->defaults->{runtime}{processed};

      my $qbt = QBTL::QBT->new( {} );

# --- Pre-check: already exists in qBittorrent? (stale queue / already added) ---
      my $exists = eval {
        my $t = $qbt->api_torrents_info_one( $hash );
        ( ref( $t ) eq 'HASH' && ( $t->{hash} // '' ) =~ /^[0-9a-f]{40}$/ )
            ? 1
            : 0;
      };

      if ( $@ ) {
        my $err = "$@";
        chomp $err;

        _add_one_mark_fail( $c->app, $hash,
                            "torrent_exists check failed: $err" );

        $processed->{$hash} = {
                              status     => 'error',
                              ts         => time,
                              last_error => "torrent_exists check failed: $err",
                              source_path => $source_path,};

        _add_one_remove_hash( $c->app, $hash );
        my $st = _add_one_state( $c->app );
        $app->log->debug(
                basename( __FILE__ ) . ":"
              . __LINE__
              . " [add_one] after remove ih: $hash idx: $st->{idx}
queue_n: "
              . scalar( @{$st->{queue} || []} ) );
        return
            $c->render(
                        template => 'qbt_add_one',
                        error    => "torrent_exists check failed: $err",
                        picked => {hash => $hash, source_path => $source_path},
            );
      }

      if ( $exists ) {

     # If it's an infohash-as-name torrent, "allow" by routing to the fix action
        my $t        = $qbt->api_torrents_info_one( $hash );
        my $qbt_name = ( ref( $t ) eq 'HASH' ) ? ( $t->{name} // '' ) : '';

        if ( length( $qbt_name ) && $qbt_name eq $hash ) {
          return $c->redirect_to( "/qbt/hashname?hash=$hash" );
        }

        my $msg =
            "Already exists in qBittorrent (stale queue or previously added):"
            . $hash;

        _add_one_mark_fail( $c->app, $hash, $msg );

        $processed->{$hash} = {
                               status      => 'exists',
                               ts          => time,
                               last_error  => $msg,
                               source_path => $source_path,};

        _add_one_remove_hash( $c->app, $hash );
        my $st = _add_one_state( $c->app );
        $app->log->debug(   basename( __FILE__ ) . ":"
                          . __LINE__
                          . " AFTER REMOVE ih: $hash idx: $st->{idx} queue_n: "
                          . scalar( @{$st->{queue} || []} ) );
        return
            $c->render(
                        template => 'qbt_add_one',
                        error    => $msg,
                        picked => {hash => $hash, source_path => $source_path},
            );
      }

      my $sz      = ( -e $source_path ) ? ( -s $source_path ) : 0;
      my $ih_file = eval { _infohash_from_torrent_file( $source_path ) };
      $ih_file = $@ ? "(ih parse failed: $@)" : $ih_file;

      $app->log->debug(   basename( __FILE__ ) . ":"
                        . __LINE__
                        . "  PREFLIGHT: size:$sz pick_hash: "
                        . short_ih( $hash )
                        . " file_ih: "
                        . short_ih( $ih_file )
                        . " path: $source_path" );

      require QBTL::SavePath;

      my ( $local_by_ih ) =
          QBTL::LocalCache::get_local_by_ih(
                                       root_dir => ( $opts->{root_dir} || '.' ),
                                       opts_local => {torrent_dir => "/"}, );
      $local_by_ih = {} if ref( $local_by_ih ) ne 'HASH';

      my ( $savepath, $why, $add );
      my $pending_root_rename_data
          ;    # <-- MUST be outside eval so we can pass to Queue

      my $ok = eval {
        my $rec = $local_by_ih->{$hash};
        die "no local record for $hash" unless ref( $rec ) eq 'HASH';

        my @sp_dbg;
        ( $savepath, $why ) =
            QBTL::SavePath::derive_savepath_from_payload( rec   => $rec,
                                                          debug => \@sp_dbg, );

        die "savepath not found ($why)\n\n" . _fmt_savepath_debug( \@sp_dbg )
            unless $savepath;

        # ---- Munge savepath + decide root rename (if hit path is known) ----
        my $hit_path = '';
        for my $ln ( @sp_dbg ) {
          next unless defined $ln;
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

        $app->log->debug(   basename( __FILE__ ) . ":"
                          . __LINE__
                          . " dbg hit_path="
                          . ( $hit_path || '(empty)' ) );

        my $torrent_top_dbg = torrent_top_lvl_from_rec( $rec );
        my $is_multi_dbg    = _is_multi_file_torrent( $rec ) ? 1 : 0;
        $app->log->debug( basename( __FILE__ ) . ":"
                . __LINE__
                . " dbg torrent_top_lvl=$torrent_top_dbg is_multi=$is_multi_dbg"
        );

        if ( $hit_path ) {    # Setup for renaming the torrent root directory
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

          $app->log->debug(   basename( __FILE__ ) . ":"
                            . __LINE__
                            . " dbg disk_file_dir=$disk_file_dir_dbg" );
          $app->log->debug(   basename( __FILE__ ) . ":"
                            . __LINE__
                            . " dbg disk_parent_dir=$disk_parent_dir_dbg" );
          $app->log->debug(   basename( __FILE__ ) . ":"
                            . __LINE__
                            . " dbg disk_root_name=$disk_root_dbg" );
          $app->log->debug(   basename( __FILE__ ) . ":"
                            . __LINE__
                            . " dbg savepath_before=$savepath" );
          $app->log->debug(   basename( __FILE__ ) . ":"
                            . __LINE__
                            . " dbg savepath_after="
                            . ( $munged_save_dbg || '(empty)' ) );

          if ( ref( $rename_dbg ) eq 'HASH' ) {
            $app->log->debug( basename( __FILE__ ) . ":"
                      . __LINE__
                      . " dbg rename_intent: "
                      . ( $rename_dbg->{torrent_top_lvl} // '(undef)' ) . " -> "
                      . ( $rename_dbg->{drivespace_top_lvl} // '(undef)' ) );

            $savepath                 = $munged_save_dbg if $munged_save_dbg;
            $pending_root_rename_data = $rename_dbg;
          }
          else {
            $app->log->debug(   basename( __FILE__ ) . ":"
                              . __LINE__
                              . " dbg rename_intent: (none)" );
          }
        }

        if ( length $hit_path ) {
          $app->log->debug( basename( __FILE__ ) . ":"
                      . __LINE__
                      . " DEBUG: about to call munge_savepath_and_root_rename  "
                      . " hit_path: $hit_path" );

          ( $savepath, $pending_root_rename_data ) =
              munge_savepath_and_root_rename(
                                              rec      => $rec,
                                              hit_path => $hit_path,
                                              savepath => $savepath, );

          if ( ref( $pending_root_rename_data ) eq 'HASH' ) {
            $rec->{pending_root_rename_data} = $pending_root_rename_data;
          }
        }

        $app->log->debug(
                  basename( __FILE__ ) . ":"
                . __LINE__
                . " savepath_final: $savepath "
                . (
              ref( $pending_root_rename_data ) eq 'HASH'
              ? " rename_root: $pending_root_rename_data->{torrent_top_lvl} -> "
                  . $pending_root_rename_data->{drivespace_top_lvl}
              : "" ) );

        $add = $qbt->api_torrents_add( $source_path, $savepath );
        die "add returned non-hash" unless ref( $add ) eq 'HASH';

        1;
      };

      unless ( $ok ) {
        $app->log->error( "DEBUG: eval failed in add_one: $@" );
        my $err = "$@";
        chomp $err;

        _add_one_mark_fail( $c->app, $hash, $err );

        $processed->{$hash} = {
                               status      => 'failed',
                               ts          => time,
                               last_error  => $err,
                               source_path => $source_path,
                               savepath    => ( $savepath || '' ),};

        _add_one_remove_hash( $c->app, $hash );
        my $st = _add_one_state( $c->app );
        $app->log->debug(
          basename( __FILE__ ) . ":" . __LINE__ . "
 [add_one] after remove ih: $hash idx: $st->{idx} queue_n: "
              . scalar( @{$st->{queue} || []} ) );
        return
            $c->render(
                        template => 'qbt_add_one',
                        error    => $err,
                        picked => {hash => $hash, source_path => $source_path},
            );
      }

      my $body = $add->{body} // '';
      $app->log->debug( basename( __FILE__ ) . ":"
        . __LINE__
        . " add_one: ok: $add->{ok} code=$add->{code} body: $body savepath: $savepath"
      );

      unless ( $add->{ok} ) {
        my $err = "Add failed: HTTP $add->{code} $body";
        _add_one_mark_fail( $c->app, $hash, $err );

        $processed->{$hash} = {
                               status      => 'failed',
                               ts          => time,
                               last_error  => $err,
                               source_path => $source_path,
                               savepath    => ( $savepath || '' ),};

        _add_one_remove_hash( $c->app, $hash );
        my $st = _add_one_state( $c->app );
        $app->log->debug( basename( __FILE__ ) . ":"
                 . __LINE__
                 . " [add_one] after remove ih: $hash idx: $st->{idx} queue_n: "
                 . scalar( @{$st->{queue} || []} ) );
        return
            $c->render(
                        template => 'qbt_add_one',
                        error    => $err,
                        picked => {hash => $hash, source_path => $source_path},
            );
      }

      if ( $body =~ /fails/i ) {
        my $err = "qBittorrent refused add: $body";
        _add_one_mark_fail( $c->app, $hash, $err );

        $processed->{$hash} = {
                               status      => 'failed',
                               ts          => time,
                               last_error  => $err,
                               source_path => $source_path,
                               savepath    => ( $savepath || '' ),};

        _add_one_remove_hash( $c->app, $hash );
        my $st = _add_one_state( $c->app );
        $app->log->debug(
          basename( __FILE__ ) . ":" . __LINE__ . "
 [add_one] after remove ih: $hash idx: $st->{idx} queue_n: "
              . scalar( @{$st->{queue} || []} ) );
        return
            $c->render(
                        template => 'qbt_add_one',
                        error    => $err,
                        picked => {hash => $hash, source_path => $source_path},
            );
      }

# ---------- hand off to Queue.pm (no inline post-check, no inline recheck) ----------
      my $pending_root_rename_data;
      my $rec = $local_by_ih->{$hash};
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

      $app->log->debug(   basename( __FILE__ ) . ":"
                        . __LINE__
                        . " rename_intent: $rr_dbg\n", );

      QBTL::Queue->enqueue_add_one(
                          $c->app, $hash,
                          source_path              => $source_path,
                          savepath                 => ( $savepath || '' ),
                          pending_root_rename_data => $pending_root_rename_data,
      );

      $processed->{$hash} = {
                             status      => 'pending',
                             ts          => time,
                             last_error  => 'queued to QBTL::Queue',
                             source_path => $source_path,
                             savepath    => ( $savepath || '' ),};

      _add_one_remove_hash( $c->app, $hash );
      my $st = _add_one_state( $c->app );

      $app->log->debug( basename( __FILE__ ) . ":"
                 . __LINE__
                 . " [add_one] after remove ih: $hash idx: $st->{idx} queue_n: "
                 . scalar( @{$st->{queue} || []} . "\n\n" ) );
      $c->app->defaults->{qbt_snap_ts} = 0;

      my $return_to = $c->param( 'return_to' ) // '';

      if (    $return_to !~ m{\A/}
           || $return_to =~ m{://}
           || $return_to =~ m{[\r\n]} )
      {
        $return_to = '/qbt/add_one';
      }

      $app->log->debug(   basename( __FILE__ ) . ":"
                        . __LINE__
                        . " add_one queued ih: $hash return_to: $return_to" );
      return $c->redirect_to( $return_to );

    } );

#   $r->post(
#     '/qbt/refresh_hashname' => sub {
#       my $c = shift;
#
#       my $qbt = QBTL::QBT->new( {} );
#
#     # simplest: just bounce back to list; list route will re-snapshot qbt anyway
#       return $c->redirect_to( '/qbt/hashnames' );
#     } );

  $r->get(
    '/qbt/hashnames' => sub {
      my $c = shift;

      $c->app->log->debug( prefix_dbg() . " ENTER /qbt/hashnames" );
      my $t0  = time;
      my $qbt = QBTL::QBT->new( {} );

      # pull fresh every time (for now)
      my $list = $qbt->api_torrents_info() || [];
      $list = [] if ref( $list ) ne 'ARRAY';

      my @rows;
      for my $t ( @$list ) {
        next unless ref( $t ) eq 'HASH';

        my $hash = $t->{hash} // '';
        my $name = $t->{name} // '';
        next unless $hash =~ /^[0-9a-f]{40}$/;
        next
            unless length( $name ) && $name eq $hash; # only “hashname” torrents

        push @rows,
            {
             hash      => $hash,
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
             prefix_dbg() . " EXIT /qbt/hashnames dt=" . ( time - $t0 ) . "s" );
      return $c->render( template => 'qbt_hashnames' );
    } );

  $r->post(
    '/qbt/refresh_hashnames' => sub {
      my $c  = shift;
      my $t0 = time;
      $c->app->log->debug( prefix_dbg() . " ENTER /qbt/hashnames" );

      # simplest “refresh” behavior: just invalidate snapshot + bounce back
      $c->app->defaults->{qbt_snap_ts} = 0;
      $c->app->log->debug(
             prefix_dbg() . " EXIT /qbt/hashnames dt=" . ( time - $t0 ) . "s" );
      return $c->redirect_to( '/qbt/hashnames' );
    } );

  $r->get(    # page view
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
     $q =~ s/^\s+|\s+$//g;

     # show=missing (default) | all
     my $show = $c->param( 'show' ) // 'missing';
     $show = ( $show eq 'all' ) ? 'all' : 'missing';

# ---------- Runtime overlay: things we already touched this server run ----------
     $c->app->defaults->{runtime} ||= {};
     $c->app->defaults->{runtime}{processed} ||=
         {};    # { ih => { status, ts, ... } }
     my $processed = $c->app->defaults->{runtime}{processed};

     # ---------- Load local cache ----------
     my ( $local_by_ih, $mtime, $src ) =
         QBTL::LocalCache::get_local_by_ih(
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
         my $qbt = QBTL::QBT->new( {} );
         $qbt_by_ih = $qbt->api_torrents_infohash_map;
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
     for my $ih ( keys %$local_by_ih ) {
       next unless defined $ih && $ih =~ /^[0-9a-f]{40}$/;
       next if exists $processed->{$ih};

       my $rec = $local_by_ih->{$ih};
       next unless ref( $rec ) eq 'HASH';

       $local_total++;

       my $exists_in_qbt = exists $qbt_by_ih->{$ih} ? 1 : 0;
       $in_qbt++ if $exists_in_qbt;
       next      if ( $show eq 'missing' ) && $exists_in_qbt;

       my $source_path = $rec->{source_path} // '';
       my $name        = $rec->{name}        // '';
       my $files       = $rec->{files};

       my $total_size = $rec->{total_size};
       $total_size = 0 if !defined $total_size;

       if ( !$name && $source_path ) {
         ( $name ) = $source_path =~ m{([^/]+)\z};
         $name ||= '';
       }

       push @rows,
           {
            hash        => $ih,
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
                ( ( $_->{hash} // '' ) =~ $re )
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

  # ---------- Idle observer tick ----------
  my $tick_seconds = 5;
  QBTL::Queue->start( $app, opts => $opts, tick_seconds => $tick_seconds );

  return $app;
}

1;

=pod

# sub _add_one_current {
#   my ( $app ) = @_;
#   my $st      = _add_one_state( $app );
#   my $q       = $st->{queue} || [];
#   return undef unless @$q;
#
#   my $i = $st->{idx} || 0;
#   $i         = 0 if $i < 0;
#   $i         = 0 if $i > $#$q;
#   $st->{idx} = $i;
#
#   return $q->[$i];
# }

# sub _add_one_meta_ref {
#   my ( $app ) = @_;
#   $app->defaults->{add_one_meta} ||= {};
#   return $app->defaults->{add_one_meta};
# }

# sub _add_one_note_fail {
#   my ( $app, $hash, $err, %extra ) = @_;
#   return unless defined $hash && $hash =~ /^[0-9a-f]{40}$/;
#
#   my $m = _add_one_meta_ref( $app );
#
#   my $rec = ( $m->{$hash} ||= {hash => $hash, retries => 0, fails => 0} );
#
#   $rec->{fails}++;
#   $rec->{failed_once} = 1 if $rec->{fails} >= 1;
#   $rec->{last_error}  = $err // '';
#   $rec->{ts_fail}     = time;
#
#   # optional breadcrumbs, no behavior
#   for my $k ( keys %extra ) { $rec->{$k} = $extra{$k} }
#
#   return $rec;
# }

# sub _add_one_note_ok {
#   my ( $app, $hash, %extra ) = @_;
#   return unless defined $hash && $hash =~ /^[0-9a-f]{40}$/;
#
#   my $m = _add_one_meta_ref( $app );
#
#   my $rec = ( $m->{$hash} ||= {hash => $hash, retries => 0, fails => 0} );
#   $rec->{ts_ok} = time;
#
#   for my $k ( keys %extra ) { $rec->{$k} = $extra{$k} }
#
#   return $rec;
# }

# sub _pending_ref {
#   my ( $app ) = @_;
#   $app->defaults->{pending} ||= {};
#   return $app->defaults->{pending};    # { ih => { ... } }
# }

# sub _pending_enqueue_add_one {
#   my ($app, $ih, %args) = @_;
#   die "bad ih" unless defined($ih) && $ih =~ /^[0-9a-f]{40}$/;
#
#   # Optional:
#   # $args{pending_root_rename_data} = { torrent_top_lvl => "...", drivespace_top_lvl => "..." }
#
#   my $pending_root_rename_data = $args{pending_root_rename_data};
#
#   my $need_root_rename =
#       (   ref($pending_root_rename_data) eq 'HASH'
#        && defined($pending_root_rename_data->{torrent_top_lvl})
#        && length($pending_root_rename_data->{torrent_top_lvl})
#        && defined($pending_root_rename_data->{drivespace_top_lvl})
#        && length($pending_root_rename_data->{drivespace_top_lvl})
#        && $pending_root_rename_data->{torrent_top_lvl} ne
#        $pending_root_rename_data->{drivespace_top_lvl}) ? 1 : 0;
#
#   my $stage =
#       $need_root_rename
#       ? 'wait_visible_then_rename'
#       : 'wait_visible_then_recheck';
#
#   _pending_upsert($app, $ih,
#                   stage                    => $stage,
#                   attempts                 => 0,
#                   next_ts                  => time,     # eligible immediately
#                   last_error               => '',
#                   pending_root_rename_data =>
#                   ($need_root_rename ? $pending_root_rename_data : undef),
#                   savepath    => ($args{savepath}    // ''),
#                   source_path => ($args{source_path} // ''),);
#
#   return;
# }
#
# sub _pending_upsert {
#   my ( $app, $ih, %patch ) = @_;
#   die "bad ih" unless defined( $ih ) && $ih =~ /^[0-9a-f]{40}$/;
#
#   my $p = _pending_ref( $app );
#   my $t = ( $p->{$ih} ||= {hash => $ih, ts_created => time} );
#
#   $t->{ts_updated} = time;
#
#   for my $k ( keys %patch ) {
#     if ( defined $patch{$k} ) { $t->{$k} = $patch{$k}; }
#     else                      { delete $t->{$k}; }  # optional: keep struct tidy
#   }
#
#   return $t;
# }
#
# sub _qbt_observer_tick {
#   my ( $app, $opts ) = @_;
#   $opts ||= {};
#
#   my $tasks = _tasks_ref( $app );
#   $app->defaults->{observer_last_tick}  = time;
#   $app->defaults->{observer_last_count} = scalar( keys %$tasks );
#   return unless %$tasks;
#
#   $app->log->debug( "[observer] tick: tasks=" . scalar( keys %$tasks ) );
#
#   my $qbt;
#   eval {
#     $qbt = QBTL::QBT->new( $opts );
#     1;
#   } or do {
#     my $err = "$@";
#     chomp $err;
#     for my $hash ( keys %$tasks ) {
#       my $t = $tasks->{$hash};
#       next unless ref( $t ) eq 'HASH';
#       $t->{stage}  = 'fail';
#       $t->{reason} = "observer: QBT init failed: $err";
#       $t->{ts}     = time;
#     }
#     return;
#   };
#
#   #   QBTL::Queue->pump_tick( $app, opts => $opts );
#   for my $hash ( keys %$tasks ) {
#     next unless $hash =~ /^[0-9a-f]{40}$/;
#
#     my $arr = eval { $qbt->api_torrents_info( hashes => $hash ) };
#     if ( $@ ) {
#       my $err = "$@";
#       chomp $err;
#       my $t = ( $tasks->{$hash} ||= {hash => $hash} );
#       $t->{stage}  = 'fail';
#       $t->{reason} = "observer: api_torrents_info failed: $err";
#       $t->{ts}     = time;
#       next;
#     }
#
#     if ( !ref( $arr ) || ref( $arr ) ne 'ARRAY' || !@$arr ) {
#       my $t = ( $tasks->{$hash} ||= {hash => $hash} );
#       $t->{stage} = 'new';
#       $t->{ts}    = time;
#       next;
#     }
#
#     my $t0       = $arr->[0];
#     my $state    = ( ref( $t0 ) eq 'HASH' ) ? ( $t0->{state}    // '' ) : '';
#     my $progress = ( ref( $t0 ) eq 'HASH' ) ? ( $t0->{progress} // -1 ) : -1;
#
#     my $t = ( $tasks->{$hash} ||= {hash => $hash} );
#     $t->{last_state}    = $state;
#     $t->{last_progress} = $progress;
#     $t->{ts}            = time;
#
#     if ( $state =~ /^checking/i ) {
#       $app->log->debug(
#                       "[observer] hash=$hash state=$state progress=$progress" );
#       $t->{stage} = 'rechecking';
#       next;
#     }
#
#     if ( defined $progress && $progress > 0 ) {
#       $t->{stage} = 'ready';
#       next;
#     }
#
#     $t->{stage} = 'suspect_zero';
#   }
#
#   return;
# }
#
# sub _qbt_snapshot {
#   my ( $app, $opts, $force ) = @_;
#   $opts  ||= {};
#   $force ||= 0;
#
#   my $ts = $app->defaults->{qbt_ts} || 0;
#
#   # choose your freshness window; 2s is plenty for UI clicks
#   my $fresh_for = 2;
#
#   if ( !$force && $ts && ( time - $ts ) <= $fresh_for ) {
#     return ( $app->defaults->{qbt_list} || [],
#              $app->defaults->{qbt_by_ih} || {} );
#   }
#
#   my $qbt  = QBTL::QBT->new( $opts );
#   my $list = $qbt->api_torrents_info() || [];
#
#   my %by;
#   for my $t ( @$list ) {
#     next if ref( $t ) ne 'HASH';
#     my $h = $t->{hash} // '';
#     next unless $h =~ /^[0-9a-fA-F]{40}$/;
#     $by{lc( $h )} = $t;
#   }
#
#   $app->defaults->{qbt_list}  = $list;
#   $app->defaults->{qbt_by_ih} = \%by;
#   $app->defaults->{qbt_ts}    = time;
#
#   return ( $list, \%by );
# }
#
# sub _shuffle_in_place {
#   my ( $a ) = @_;
#   return $a unless ref( $a ) eq 'ARRAY';
#   for ( my $i = @$a - 1 ; $i > 0 ; $i-- ) {
#     my $j = int( rand( $i + 1 ) );
#     next if $i == $j;
#     @$a[ $i, $j ] = @$a[ $j, $i ];
#   }
#   return $a;
# }

# sub _qbt_snapshot_refresh {
#   my ( $app, $qbt ) = @_;
#   my $snap = _qbt_snapshot( $app );
#   my $by   = $qbt->api_torrents_infohash();
#   $by            = {} if ref( $by ) ne 'HASH';
#   $snap->{by_ih} = $by;
#   $snap->{ts}    = time;
#   return $by;
# }
#
# sub _qbt_snapshot_put_one {
#   my ( $app, $hash, $t ) = @_;
#   return unless defined $hash && $hash =~ /^[0-9a-f]{40}$/;
#   return unless ref( $t ) eq 'HASH';
#   my $snap = _qbt_snapshot( $app );
#   $snap->{by_ih}{$hash} = $t;
#   $snap->{ts} = time;
#   return;
# }
#
# sub _task_counts {
#   my ( $app ) = @_;
#   my $tasks = _tasks_ref( $app );
#
#   my %counts;
#   for my $hash ( keys %$tasks ) {
#     my $t = $tasks->{$hash};
#     next unless ref( $t ) eq 'HASH';
#     my $stage = $t->{stage} || 'unknown';
#     $counts{$stage}++;
#   }
#
#   my $total = scalar( keys %$tasks );
#   return ( $total, \%counts );
# }
#
# sub _task_fail_items {
#   my ( $app, $limit ) = @_;
#   $limit ||= 200;
#
#   my $tasks = _tasks_ref( $app );
#   my @fails;
#
#   for my $hash ( keys %$tasks ) {
#     my $t = $tasks->{$hash};
#     next unless ref( $t ) eq 'HASH';
#     next unless ( $t->{stage} || '' ) eq 'fail';
#     push @fails, $t;
#   }
#
#   @fails = sort { ( $b->{ts} || 0 ) <=> ( $a->{ts} || 0 ) } @fails;
#
#   @fails = @fails[ 0 .. ( $limit - 1 ) ] if @fails > $limit;
#   return \@fails;
# }
#
# sub _tasks_ref {
#   my ( $app ) = @_;
#   $app->defaults->{tasks} ||= {};
#   return $app->defaults->{tasks};
# }
#
# sub _task_upsert {
#   my ( $app, $hash, %patch ) = @_;
#   $hash = lc( $hash // '' );
#
#   $app->defaults->{tasks} ||= {};
#   my $t =
#       ( $app->defaults->{tasks}{$hash} ||=
#         {hash => $hash, ts_created => time} );
#
#   $t->{ts_updated} = time;
#   @$t{keys %patch} = values %patch;
#
#   return $t;
# }

# ---------- Idle observer tick ----------

#   my $tick_seconds = 5;
#
#   #   QBTL::Queue->start( $app, opts => $opts, tick_seconds => $tick_seconds );
#
#   Mojo::IOLoop->recurring(
#     $tick_seconds => sub {
#       eval { QBTL::Queue->pump_tick( $app, opts => $opts ); 1 } or do {
#         my $err = "$@";
#         chomp $err;
#         $app->log->error( __LINE__ . " [Web] pump_tick died: $err" );
#       };
#     } );
#   return $app;
#}

1;


=cut
