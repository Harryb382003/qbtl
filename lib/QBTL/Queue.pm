package QBTL::Queue;

use common::sense;
use Mojo::IOLoop;

# ------------------------------
# Public API
# ------------------------------

sub enqueue_add_one {
  my ( $class, $app, $ih, %args ) = @_;
  die "bad ih" unless defined( $ih ) && $ih =~ /^[0-9a-f]{40}$/;

  my $jobs = _jobs_ref( $app );

  my $job = (
              $jobs->{$ih} ||= {
                                ih         => $ih,
                                steps      => [],
                                step_idx   => 0,
                                status     => 'queued',
                                stage      => '',
                                next_ts    => time,
                                ts_created => time,
                                ts_updated => time,} );

  $job->{ts_updated} = time;

  # qBt keyword purity (store what we pass to qBt using qBt names)
  $job->{source_path} = $args{source_path} if defined $args{source_path};

  # qBt param is "savepath" (field for torrents/add)
  $job->{savepath} = $args{savepath} if defined $args{savepath};

  # rename intent (optional)
  if ( ref( $args{pending_root_rename_data} ) eq 'HASH' ) {
    $job->{rename} = $args{pending_root_rename_data};
  }

  # (re)build steps if empty OR if we were re-queued from scratch
  if ( !@{$job->{steps} || []}
       || ( $job->{status} // '' ) =~ /^(queued|failed|done)$/ )
  {
    $job->{steps}    = _build_steps_add_one( $job );
    $job->{step_idx} = 0;
    $job->{stage}    = 'queued';
    $job->{next_ts}  = time + 0.5;
  }
  else {
    $job->{next_ts} = time + 0.5 if ( $job->{next_ts} || 0 ) < time;
  }

  my $rename_dbg = '(none)';
  if ( ref( $job->{rename} ) eq 'HASH' ) {
    my $from = $job->{rename}{torrent_top_lvl}    // '?';
    my $to   = $job->{rename}{drivespace_top_lvl} // '?';
    $rename_dbg = "$from -> $to";
  }

  $app->log->debug( "[queue] enqueue_add_one ih=$ih rename=$rename_dbg" );
  return $job;
}

sub start {
  my ( $class, $app, %args ) = @_;
  my $opts         = $args{opts}         || {};
  my $tick_seconds = $args{tick_seconds} || 5;

  my $idref = _tick_id_ref( $app );
  return if $$idref;    # already running

  my $id = Mojo::IOLoop->recurring(
    $tick_seconds => sub {
      eval { $class->pump_tick( $app, opts => $opts ); 1 } or do {
        my $err = "$@";
        chomp $err;
        $app->log->error( "[queue] pump_tick died: $err" );
      };
    } );

  $$idref = $id;
  $app->log->debug( " [queue] start: tick id=$id" );
  return $id;
}

sub stop {
  my ( $class, $app ) = @_;
  my $idref = _tick_id_ref( $app );
  return unless $$idref;

  Mojo::IOLoop->remove( $$idref );
  $app->log->debug( " [queue] stop: tick id=$$idref" );
  $$idref = undef;
  return 1;
}

# ------------------------------
# State storage
# ------------------------------

sub _jobs_ref {
  my ( $app ) = @_;
  $app->defaults->{queue_jobs} ||= {};
  return $app->defaults->{queue_jobs};
}

sub _tick_id_ref {
  my ( $app ) = @_;
  $app->defaults->{queue_tick_id} ||= undef;
  return \$app->defaults->{queue_tick_id};
}

# ------------------------------
# Planning
# ------------------------------

sub _build_steps_add_one {
  my ( $job ) = @_;
  my @steps;

  push @steps, {op => 'wait_visible', tries => 0};

  if ( ref( $job->{rename} ) eq 'HASH' ) {
    push @steps, {op => 'rename_root',    tries => 0};
    push @steps, {op => 'confirm_rename', tries => 0};
  }

  push @steps, {op => 'recheck',          tries => 0};
  push @steps, {op => 'confirm_checking', tries => 0};

  return \@steps;
}

# ------------------------------
# Tick runner
# ------------------------------

sub pump_tick {
  my ( $class, $app, %args ) = @_;
  my $opts = $args{opts} || {};

  my $jobs = _jobs_ref( $app );

  #   $app->log->debug( "[queue] pump_tick: jobs=" . scalar( keys %$jobs ) );
  return unless %$jobs;

  my $qbt;
  my $qbt_ok = eval { $qbt = QBTL::QBT->new(); 1 };
  if ( !$qbt_ok ) {
    my $err = "$@";
    chomp $err;
    $app->log->error( "[queue] QBT init failed: $err" );
    return;
  }

  my $now        = time;
  my $tick_cache = {};     # cache for this pump_tick call

  for my $ih ( sort keys %$jobs ) {
    my $job = $jobs->{$ih};
    next if ref( $job ) ne 'HASH';

    my $status   = $job->{status}   // '';
    my $stage    = $job->{stage}    // '';
    my $step_idx = $job->{step_idx} // 0;
    my $next_ts  = $job->{next_ts}  // 0;

#     $app->log->debug(
#          sprintf(
#            "Q->[ JOB ]  ih=%s status=%s stage=%s step_idx=%s next_ts=%s now=%s",
#            $ih, $status, $stage, $step_idx, $next_ts, $now ) );

    next if $next_ts > $now;
    next if $status =~ /^(done|failed)$/;

    my $steps = $job->{steps};
    if ( ref( $steps ) ne 'ARRAY' ) {
      $steps = [];
      $job->{steps} = $steps;
    }

    # Show REMAINING plan (not the whole original list)
    if ( @$steps ) {
      my @remain = ();
      if ( $step_idx < @$steps ) {
        @remain = @$steps[ $step_idx .. $#$steps ];
      }

      my $ops = join(
                     ",",
                     map { ( ref( $_ ) eq 'HASH' ) ? ( $_->{op} // "?" ) : "?" }
                          @remain );

      $app->log->debug(   "Q->[ PLAN ] ih=$ih step_idx=$step_idx steps="
                        . scalar( @$steps )
                        . " remaining=[$ops]" );
    }
    else {
      $app->log->debug(
                 "Q->[ PLAN ] ih=$ih step_idx=$step_idx steps=0 remaining=[]" );
    }

    if ( $step_idx >= @$steps ) {
      _job_mark_done( $job, $now );
      next;
    }

    my $step = $steps->[$step_idx];
    if ( ref( $step ) ne 'HASH' ) {
      _job_mark_failed( $job, $now, "bad step at idx=$step_idx (not a HASH)" );
      next;
    }

    $step->{tries} = 0 if !defined $step->{tries};

    my $op = $step->{op} // '';
    $job->{stage} = $op;

    my ( $result, $why ) = _run_step( $app, $qbt, $job, $step, $tick_cache );
    $app->log->debug( "[queue] step ih=$ih op=$op result=$result why="
                      . ( defined( $why ) ? $why : '(undef)' ) );

    if ( $result eq 'advance' ) {
      _job_advance_step( $job, $now );
      next;
    }

    if ( $result eq 'retry' ) {
      _job_retry_step( $app, $job, $step, $now, $why );
      next;
    }

    _job_mark_failed( $job, $now, $why || "failed (result=$result)" );
  }

  return;
}

# ------------------------------
# Job state helpers
# ------------------------------
sub _cleanup_symlink_if_any {
  my ( $app, $job ) = @_;
  my $link = $job->{symlink_path} // '';
  return 0 unless length $link;

  if ( -l $link ) {
    my $ok = unlink $link;
    $app->log->debug( "[queue] symlink cleanup link=$link ok=$ok" );
  }
  else {
    $app->log->debug(
                 "[queue] symlink cleanup skipped (not a symlink) link=$link" );
  }

  delete $job->{symlink_path};
  delete $job->{symlink_target};
  return 1;
}

sub _job_mark_done {
  my ( $job, $now ) = @_;
  $job->{status}     = 'done';
  $job->{stage}      = '';
  $job->{last_error} = '';
  $job->{next_ts}    = $now + 9999999;
  $job->{ts_updated} = $now;
  return 1;
}

sub _job_mark_failed {
  my ( $job, $now, $why ) = @_;
  $job->{status}     = 'failed';
  $job->{stage}      = $job->{stage} // '';
  $job->{last_error} = $why || 'failed';
  $job->{next_ts}    = $now + 9999999;
  $job->{ts_updated} = $now;
  return 1;
}

sub _job_advance_step {
  my ( $job, $now ) = @_;

  my $steps    = $job->{steps}    || [];
  my $step_idx = $job->{step_idx} || 0;

  $job->{step_idx}   = $step_idx + 1;
  $job->{status}     = 'running';
  $job->{last_error} = '';
  $job->{ts_updated} = $now;
  $job->{next_ts}    = $now;

  if ( $job->{step_idx} >= @$steps ) {
    $job->{stage} = '';
  }
  else {
    my $next_step = $steps->[ $job->{step_idx} ];
    my $next_op =
        ( ref( $next_step ) eq 'HASH' ) ? ( $next_step->{op} // '' ) : '';
    $job->{stage} = $next_op;

    if ( ref( $next_step ) eq 'HASH' ) {
      $next_step->{tries} = 0 if !defined $next_step->{tries};
    }
  }

  return 1;
}

sub _job_retry_step {
  my ( $app, $job, $step, $now, $why ) = @_;

  $step->{tries}++;
  my $t  = $step->{tries};
  my $op = $step->{op} // '';

  # --- TEST MODE: confirm_checking bounce back to recheck after N attempts ---
  if ( $op eq 'confirm_checking' ) {
    my $N = $app->defaults->{confirm_checking_bounce_n} || 6;

    if ( $t >= $N ) {
      my $steps = $job->{steps} || [];
      if ( ref( $steps ) eq 'ARRAY' && @$steps ) {
        for ( my $i = 0 ; $i < @$steps ; $i++ ) {
          next unless ref( $steps->[$i] ) eq 'HASH';
          next unless ( $steps->[$i]{op} // '' ) eq 'recheck';

          # clear recheck latch so it will actually call the API again
          delete $steps->[$i]{recheck_requested};
          delete $job->{recheck_requested_ts};
          delete $job->{recheck_last_check0};

          # reset confirm tries and bounce job back to recheck
          $step->{tries}     = 0;
          $job->{step_idx}   = $i;
          $job->{status}     = 'running';
          $job->{stage}      = 'recheck';
          $job->{last_error} = $why || 'retry';
          $job->{ts_updated} = $now;
          $job->{next_ts}    = $now + 1;

          $app->log->debug(
"[queue] bounce ih=$job->{ih} confirm_checking->recheck after ${N}x why="
                . ( $job->{last_error} // '' ) );

          return 1;
        }
      }

      # if we can't find recheck step, fall through to normal retry logic
    }
  }

  my $delay;

  if ( $op eq 'wait_visible' ) {
    $delay = 0.5 * ( 2**( $t - 1 ) );
    $delay = 10 if $delay > 10;
  }
  elsif ( $op eq 'rename_root' || $op eq 'confirm_rename' ) {
    $delay = 2 * ( 2**( $t - 1 ) );
    $delay = 60 if $delay > 60;
  }
  elsif ( $op eq 'confirm_checking' ) {
    my @sched = ( 10, 30, 60, 120, 300 );
    $delay = $sched[ $t - 1 ] // 300;
  }
  else {
    $delay = 1 * ( 2**( $t - 1 ) );
    $delay = 60 if $delay > 60;
  }

  my $max = $step->{max_tries} // 0;
  if ( $max && $t >= $max ) {
    _job_mark_failed( $job, $now,
                      "step $op exceeded max_tries=$max: " . ( $why // '' ) );
    return 1;
  }

  $job->{next_ts}    = $now + $delay;
  $job->{status}     = 'running';
  $job->{stage}      = $op;
  $job->{last_error} = $why || 'retry';
  $job->{ts_updated} = $now;

  if ( $delay >= 30 || ( $t % 5 ) == 0 ) {
    $app->log->debug(
"[queue] retry ih=$job->{ih} op=$op tries=$t next_in=${delay}s why=$job->{last_error}"
    );
  }

  return 1;
}

# ------------------------------
# Step runner
# ------------------------------

sub _qbt_torrents_info_one__cached {
  my ( $qbt, $cache, $ih ) = @_;
  die "missing cache" unless ref( $cache ) eq 'HASH';
  die "bad ih"        unless defined( $ih ) && $ih =~ /^[0-9a-f]{40}$/;

  if ( exists $cache->{torrents_info_one}{$ih} ) {
    return $cache->{torrents_info_one}{$ih};
  }

  my $t = $qbt->api_torrents_info_one( $ih );
  $cache->{torrents_info_one}{$ih} = $t;
  return $t;
}

sub _run_step {
  my ( $app, $qbt, $job, $step, $tick_cache ) = @_;
  $tick_cache ||= {};
  my $op = $step->{op} // '';

  my $ih = $job->{ih} // '';
  return ( 'fail', "bad ih in job (missing/invalid)" )
      unless $ih =~ /^[0-9a-f]{40}$/;

  if ( $op eq 'wait_visible' ) {
    my $t = _qbt_torrents_info_one__cached( $qbt, $tick_cache, $ih );
    if ( ref( $t ) eq 'HASH' && ( $t->{hash} // '' ) =~ /^[0-9a-f]{40}$/ ) {
      return ( 'advance', 'wait_visible: now visible' );
    }
    return ( 'retry', "wait_visible: not visible yet" );
  }

  if ( $op eq 'rename_root' ) {
    my $rr = $job->{rename};
    return ( 'advance', 'rename_root: no rename requested' )
        unless ref( $rr ) eq 'HASH';

    my $oldPath = $rr->{torrent_top_lvl}    // '';
    my $newPath = $rr->{drivespace_top_lvl} // '';

    return ( 'advance', 'rename_root: missing paths' )
        unless length( $oldPath ) && length( $newPath );
    return ( 'advance', 'rename_root: old==new' ) if $oldPath eq $newPath;

    return ( 'advance', 'rename_root: already requested' )
        if $step->{rename_requested};

    my $res = $qbt->api_torrents_renameFolder( $ih, $oldPath, $newPath );

    if ( ref( $res ) eq 'HASH' && $res->{ok} ) {
      $step->{rename_requested} = 1;
      return ( 'advance', 'rename_root: requested ok' );
    }

    my $code = ( ref( $res ) eq 'HASH' ) ? ( $res->{code} // 0 )  : 0;
    my $body = ( ref( $res ) eq 'HASH' ) ? ( $res->{body} // '' ) : '';

    if ( $code == 409 && $body =~ /already exists/i ) {
      $step->{rename_requested} = 1;
      return ( 'advance', "rename_root: 409 already-exists; polling confirm" );
    }

    return ( 'retry',
             "rename_root: renameFolder not ok: http=$code body=[$body]" );
  }

  if ( $op eq 'confirm_rename' ) {
    my $rr = $job->{rename};
    return ( 'advance', 'confirm_rename: no rename requested' )
        unless ref( $rr ) eq 'HASH';

    my $newRoot = $rr->{drivespace_top_lvl} // '';
    return ( 'advance', 'confirm_rename: missing newRoot' )
        unless length( $newRoot );

    my $arr = $qbt->api_torrents_files( $ih );
    return ( 'retry', "confirm_rename: torrents/files did not return ARRAY" )
        unless ref( $arr ) eq 'ARRAY';

    for my $f ( @$arr ) {
      next unless ref( $f ) eq 'HASH';
      my $p = $f->{name} // $f->{path} // '';
      next unless length $p;
      if ( $p =~ /^\Q$newRoot\E\// ) {
        return ( 'advance', "confirm_rename: confirmed newRoot=$newRoot" );
      }
    }

    return ( 'retry', "confirm_rename: unconfirmed yet (want=$newRoot)" );
  }

  if ( $op eq 'recheck' ) {
    return ( 'advance', 'recheck: already requested' )
        if $step->{recheck_requested};

    my $t0 = _qbt_torrents_info_one__cached( $qbt, $tick_cache, $ih );
    if ( ref( $t0 ) eq 'HASH' && exists $t0->{last_check} ) {
      $job->{recheck_last_check0} = $t0->{last_check};
    }

    # Optional debug: what qbt thinks right before recheck
    if ( ref( $t0 ) eq 'HASH' ) {
      $app->log->debug(   "[queue] pre-recheck ih=$ih state="
                        . ( $t0->{state} // '' )
                        . " save_path="
                        . ( $t0->{save_path} // '' )
                        . " download_path="
                        . ( $t0->{download_path} // '' ) );
    }

    my $sp = ( ref( $t0 ) eq 'HASH' ) ? ( $t0->{save_path} // '' ) : '';
    if ( length $sp ) {
      my $r1 = $qbt->api_torrents_setLocation( $ih, $sp );
      if ( ref( $r1 ) eq 'HASH' && !$r1->{ok} ) {
        return ( 'retry',
            "recheck: setLocation failed http=$r1->{code} body=[$r1->{body}]" );
      }

      # IMPORTANT:
      # setDownloadPath is frequently unsupported/400 on some qBt versions.
      # For YOUR recovery use-case, it is not required to successfully recheck.
      # Keep it OFF by default. If you want to experiment, set:
      #   $app->defaults->{try_set_download_path} = 1;
      if ( $app->defaults->{try_set_download_path} ) {
        my $r2 = $qbt->api_torrents_setDownloadPath( $ih, $sp );
        if ( ref( $r2 ) eq 'HASH' && !$r2->{ok} ) {
          return (
            'retry',
"recheck: setDownloadPath failed http=$r2->{code} body=[$r2->{body}]"
          );
        }
      }
    }

    my $r = $qbt->api_torrents_recheck( $ih );
    if ( ref( $r ) ne 'HASH' || !$r->{ok} ) {
      my $code = ( ref( $r ) eq 'HASH' ) ? ( $r->{code} // 0 )  : 0;
      my $body = ( ref( $r ) eq 'HASH' ) ? ( $r->{body} // '' ) : '';
      return ( 'retry', "recheck: recheck not ok: http=$code body=[$body]" );
    }

    $job->{recheck_requested_ts} = time;
    $step->{recheck_requested}   = 1;
    delete $tick_cache->{torrents_info_one}{$ih};

    my $t1 = _qbt_torrents_info_one__cached( $qbt, $tick_cache, $ih );
    if ( ref( $t1 ) eq 'HASH' ) {
      $app->log->debug(   "[queue] post-recheck ih=$ih state="
                        . ( $t1->{state} // '' )
                        . " save_path="
                        . ( $t1->{save_path} // '' )
                        . " download_path="
                        . ( $t1->{download_path} // '' ) );
    }

    return ( 'advance', 'recheck: requested ok' );
  }

  if ( $op eq 'confirm_checking' ) {
    my $req_ts  = $job->{recheck_requested_ts} // 0;
    my $base_lc = $job->{recheck_last_check0};

    my $t0 = _qbt_torrents_info_one__cached( $qbt, $tick_cache, $ih );
    return ( 'retry', "confirm_checking: not visible yet" )
        unless ref( $t0 ) eq 'HASH';

    my $state      = $t0->{state} // '';
    my $progress   = exists $t0->{progress}   ? $t0->{progress}   : undef;
    my $last_check = exists $t0->{last_check} ? $t0->{last_check} : undef;

    if ( $state =~
/^(checking|checkingResumeData|queuedForChecking|queuedForCheck|allocating)/i
        )
    {
      return ( 'advance', "confirm_checking: now checking (state=$state)" );
    }

    if ( defined $last_check ) {
      my $lc = $last_check;
      $lc = 0 if !defined $lc || $lc < 0;

      if ( defined $base_lc ) {
        my $b = $base_lc;
        $b = 0 if !defined $b || $b < 0;
        if ( $lc > $b ) {
          return ( 'advance',
                   "confirm_checking: last_check advanced ($b -> $lc)" );
        }
      }
      elsif ( $req_ts ) {
        if ( $lc >= $req_ts ) {
          return ( 'advance',
                "confirm_checking: last_check >= request_ts ($lc >= $req_ts)" );
        }
      }
    }

    $app->log->debug(
                      "[queue] confirm_checking ih=$ih state=$state last_check="
                          . ( defined( $last_check ) ? $last_check : '(undef)' )
                          . " base_lc="
                          . ( defined( $base_lc ) ? $base_lc : '(undef)' )
                          . " req_ts=$req_ts progress="
                          . ( defined( $progress ) ? $progress : '(undef)' ) );

    return (
             'retry',
             "confirm_checking: not reflected yet (state=$state"
                 . " last_check="
                 . ( defined( $last_check ) ? $last_check : '(undef)' )
                 . " progress="
                 . ( defined( $progress ) ? $progress : '(undef)' ) . ")" );
  }

  return ( 'fail', "unknown op=$op" );
}

1;
