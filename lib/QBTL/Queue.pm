package QBTL::Queue;

use common::sense;
use Cwd   qw( abs_path );
use POSIX qw(strftime);
use Mojo::IOLoop;

use QBTL::QBT;
use QBTL::SavePath qw( derive_savepath_from_payload );
use QBTL::Utils    qw(
    short_ih
    epoch2time
    prefix_dbg
);

use File::Basename qw( dirname basename);

my %REQUEUE_POLICY = (
  renamed_root_class => sub {
    my ( $app, $qbt, $tick_cache, $job ) = @_;
    my $enabled = $app->defaults->{qbt}{prefs}{temp_path_enabled};
    return 0 if !defined( $enabled ) || $enabled;    # only when disabled
    return 1;
  },

  # later:
  # no_disk_space => sub { ... },
  # needs_config  => sub { ... },
);

# ------------------------------
# Public API
# ------------------------------

sub start {
  my ( $class, $app, %args ) = @_;
  my $opts         = $args{opts}         || {};
  my $tick_seconds = $args{tick_seconds} || 5;
  $app->log->format(
    sub {
      my ( $time, $level, @lines ) = @_;

      my $ts = strftime( '%m-%d %H:%M', localtime( $time ) );

      return sprintf "[%s] [%d] [%s] %s\n", $ts, $$, $level, join "\n", @lines;
    } );

  my $idref = _tick_id_ref( $app );
  return if $$idref;
  $app->log->debug(
                   prefix_dbg() . " start called tick_seconds: $tick_seconds" );
  my $id = Mojo::IOLoop->recurring(
    $tick_seconds => sub {
      eval { $class->pump_tick( $app, opts => $opts ); 1 } or do {
        my $err = "$@";
        chomp $err;
        $app->log->error( prefix_dbg() . " pump_tick died: $err" );
      };
    } );

  $$idref = $id;
  $app->log->debug( prefix_dbg() . " start: tick id: $id" );
  return $id;
}

sub stop {
  my ( $class, $app ) = @_;
  my $idref = _tick_id_ref( $app );
  return unless $$idref;

  Mojo::IOLoop->remove( $$idref );
  $app->log->debug( prefix_dbg() . " stop: tick id: $$idref" );
  $$idref = undef;
  return 1;
}

# ------------------------------
# State storage
# ------------------------------

sub _bless_paths {
  my ( $app, $qbt, $job, $tick_cache, %opt ) = @_;
  $tick_cache ||= {};

  return 0 unless ref( $job ) eq 'HASH';
  return 1 if $job->{paths_blessed} && !$opt{force};

  my $ih = $job->{ih} // '';
  return 0 unless $ih =~ /^[0-9a-f]{40}$/;

  my $t0 = $opt{t0} || _qbt_torrents_info_one_cached( $qbt, $tick_cache, $ih );
  return 0 unless ref( $t0 ) eq 'HASH';

  my $save_path     = $t0->{save_path}     // '';
  my $download_path = $t0->{download_path} // '';
  my $state         = $t0->{state}         // '';
  my $last_check    = exists $t0->{last_check} ? $t0->{last_check} : undef;
  my $progress      = exists $t0->{progress}   ? $t0->{progress}   : undef;

  # Derive "root" the same way recheck does (so we bless what we actually used)
  my $root = '';

  if ( ref( $job->{rename} ) eq 'HASH' ) {
    $root = $job->{rename}{drivespace_top_lvl} // '';
    $root ||= $job->{rename}{torrent_top_lvl} // '';
  }

  if ( !length( $root ) ) {
    my $arr = eval { $qbt->api_torrents_files( $ih ) };
    if ( ref( $arr ) eq 'ARRAY' && @$arr ) {
      my $p = '';
      for my $f ( @$arr ) {
        next unless ref( $f ) eq 'HASH';
        $p = $f->{name} // $f->{path} // '';
        last if length $p;
      }
      if ( length $p ) {
        ( $root ) = split m{/}, $p, 2;
        $root ||= '';
      }
    }
  }

  $job->{blessed_paths} = {
    ih            => $ih,
    ts            => time,
    state         => $state,
    save_path     => $save_path,
    download_path => $download_path,
    root          => $root,
    last_check    => $last_check,
    progress      => $progress,

    symlink_used   => $job->{symlink_used} ? 1 : 0,
    symlink_path   => $job->{symlink_path}   // '',
    symlink_target => $job->{symlink_target} // '',};

  $job->{blessed_paths_ts} = $job->{blessed_paths}{ts};

  # CANONICAL LATCH
  $job->{paths_blessed} = 1;

  $app->log->debug( prefix_dbg()
         . "  bless_paths ih="
         . short_ih( $ih )
         . " state=$state save=[$save_path] dl=[$download_path] root=[$root]" );
  $app->log->debug( prefix_dbg() . "  PATHS ARE NOW BLESSED" );

  return 1;
}

sub _jobs_ref {
  my ( $app ) = @_;
  $app->defaults->{queue_jobs} ||= {};
  return $app->defaults->{queue_jobs};
}

sub _gui_ref {
  my ( $app ) = @_;
  $app->defaults->{queue_gui} ||= {};
  return $app->defaults->{queue_gui};
}

sub _tick_id_ref {
  my ( $app ) = @_;
  $app->defaults->{queue_tick_id} ||= undef;
  return \$app->defaults->{queue_tick_id};
}

sub _jobs_summary {
  my ( $jobs ) = @_;
  $jobs ||= {};

  my %n;

  for my $ih ( keys %$jobs ) {
    my $job = $jobs->{$ih};
    next unless ref( $job ) eq 'HASH';

    my $st = $job->{status} // '';

    # ZEN (optional): keep it out of "jobs="
    if ( $st eq 'done' ) { $n{ZEN}++; next; }

    # ACTIVE: currently runnable / in-flight
    if ( $st =~ /^(queued|running)$/ ) { $n{ACTIVE}++; next; }

    # Named issue buckets
    if ( $st eq 'needs_user' )   { $n{INTERACTIVE}++;  next; }
    if ( $st eq 'renamed_root' ) { $n{RENAMED_ROOT}++; next; }
    if ( $st eq 'failed' )       { $n{FAILED}++;       next; }

    # Everything else is triage until named
    $n{TRIAGE}++;
  }

  # "jobs=" is sum of all non-ZEN buckets (your A)
  my $jobs_pending = 0;
  $jobs_pending += $n{$_} for grep { $_ ne 'ZEN' } keys %n;

  # Build brief, dynamic line (hide 0/noise)
  my @parts;
  push @parts, "jobs=$jobs_pending";
  for my $k ( qw(ACTIVE INTERACTIVE RENAMED_ROOT FAILED TRIAGE) ) {
    push @parts, "$k=$n{$k}" if $n{$k};
  }

  # optional ZEN: include only if non-zero
  push @parts, "ZEN: $n{ZEN}" if $n{ZEN};

  return join( " ", @parts );
}

# ------------------------------
# Planning
# ------------------------------
sub enqueue_add_one {
  my ( $class, $app, $ih, %args ) = @_;
  die "bad ih" unless defined( $ih ) && $ih =~ /^[0-9a-f]{40}$/;
  $app->log->debug(
             prefix_dbg() . " Entered enqueue_add_one ih: " . short_ih( $ih ) );
  my $jobs = _jobs_ref( $app );
  my $now  = time;

  my $job = (
              $jobs->{$ih} ||= {
                                ih         => $ih,
                                steps      => [],
                                step_idx   => 0,
                                status     => 'queued',
                                stage      => '',
                                next_ts    => $now,
                                ts_created => $now,
                                ts_updated => $now,} );

# Bridge team latch (per-run invariant):
# Bridge/symlink only exists when rename_root_required=1 && enabled=1
# We treat that whole worldview as a "team": if it fails, cleanup/delete is allowed;
# if not, init must never do destructive/cleanup actions.
  $job->{bridged} =
      ( ( ref( $job->{rename} ) eq 'HASH' ) && ( $args{enabled} ? 1 : 0 ) )
      ? 1
      : 0;

  $job->{ts_updated} = $now;

  # qBt keyword purity (store what we pass to qBt using qBt names)
  $job->{source_path} = $args{source_path} if defined $args{source_path};

  # qBt param is " savepath " (field for torrents/add)
  $job->{savepath} = $args{savepath} if defined $args{savepath};

  # rename intent (optional)
  if ( ref( $args{pending_root_rename_data} ) eq 'HASH' ) {
    $job->{rename} = $args{pending_root_rename_data};
  }

  my $status = $job->{status} // '';
  if ( !@{$job->{steps} || []}
       || $status =~ /^(queued|failed|done|needs_user)$/ )
  {
    $job->{steps}       = [];    # defer planning until pump_tick latches prefs
    $job->{step_idx}    = 0;
    $job->{plan_needed} = 1;
    $job->{stage}       = 'queued';
    $job->{status}      = 'queued';
    $job->{next_ts}     = $now + 0.5;
    $job->{plan_needed} = 1;

    # clear per-run latches that must not survive a re-queue
    delete $job->{recheck_requested_ts};
    delete $job->{recheck_last_check0};
    delete $job->{blessed_paths};
    delete $job->{blessed_paths_ts};

    # clear symlink bookkeeping too
    delete $job->{symlink_path};
    delete $job->{symlink_target};
    delete $job->{symlink_used};
    delete $job->{terminal_logged};
  }
  else {
    # job already in-flight; just make sure it can run soon
    $job->{next_ts} = $now + 0.5 if ( $job->{next_ts} || 0 ) < $now;
  }

  my $rename_dbg = '(none)';
  if ( ref( $job->{rename} ) eq 'HASH' ) {
    my $from = $job->{rename}{torrent_top_lvl}    // '?';
    my $to   = $job->{rename}{drivespace_top_lvl} // '?';
    $rename_dbg = "$from->$to ";
  }

  $app->log->debug(   prefix_dbg()
                    . "  enqueue_add_one ih = "
                    . short_ih( $ih )
                    . " rename = $rename_dbg " );
  return $job;
}

sub _build_steps_add_one {
  my ( $job ) = @_;
  my @steps;

  # You must have this latched on the job *before* building steps.
  # (set it in pump_tick when you read prefs)
  my $enabled = $job->{temp_path_enabled};

  my $rename_required = ( ref( $job->{rename} ) eq 'HASH' ) ? 1 : 0;

# LOCKED: bridge/symlink is only created when rename_root_required=1 && enabled=1
  my $bridged = ( $enabled && $rename_required ) ? 1 : 0;

  push @steps, {op => 'wait_visible', tries => 0};

  if ( $rename_required ) {
    push @steps, {op => 'rename_root',    tries => 0};
    push @steps, {op => 'confirm_rename', tries => 0};
  }

  # LOCKED: all threads end with check -> confirm -> zen || classify
  push @steps, {op => 'recheck',          tries => 0};
  push @steps, {op => 'confirm_checking', tries => 0};

# If no bridge is needed, INIT MUST BE ABLE TO STOP HERE (Init Zen / handed off)
  return \@steps unless $bridged;

  # ----This only happens when the symlink bridge was required ----
  if ( $bridged ) {
    push @steps, {op => 'remove_symlink',       tries => 0};
    push @steps, {op => 'recheck_again',        tries => 0};
    push @steps, {op => 'confirm_not_checking', tries => 0};
    push @steps, {op => 'interactive',          tries => 0};
    push @steps, {op => 'delete_torrent',       tries => 0};
  }
  return \@steps;
}

sub _renamed_root_ref {
  my ( $app ) = @_;
  $app->defaults->{queue_renamed_root} ||= {};
  return $app->defaults->{queue_renamed_root};
}

sub _bridged_now {
  my ( $job ) = @_;
  return undef unless ref( $job ) eq 'HASH';

  my $tpe = $job->{temp_path_enabled};    # undef|0|1
  return undef unless defined $tpe;

  my $rename_required = ( ref( $job->{rename} ) eq 'HASH' ) ? 1 : 0;
  return ( $tpe && $rename_required ) ? 1 : 0;
}

# ------------------------------
# Tick runner
# ------------------------------
sub pump_tick {
  my ( $class, $app, %args ) = @_;
  state $TICK_N = 0;
  $TICK_N++;
  my $opts = $args{opts} || {};
  my $jobs = _jobs_ref( $app );
  my $sum  = _jobs_summary( $jobs );

  return unless %$jobs;

  my $qbt;
  my $qbt_ok = eval { $qbt = QBTL::QBT->new(); 1 };
  unless ( $qbt_ok ) {
    my $err = "$@";
    chomp $err;
    $app->log->error( "[queue] QBT init failed: $err" );
    return;
  }

  my $now        = time;
  my $tick_cache = {};     # per invocation cache

  # ---- qBt prefs snapshot (once per tick) ----
  my $temp_enabled = undef;    # 1|0|undef
  my $prefs_ok     = eval {
    my $prefs = $qbt->api_app_preferences();    # HASH
    if ( ref( $prefs ) eq 'HASH' && exists $prefs->{temp_path_enabled} ) {
      $temp_enabled = $prefs->{temp_path_enabled} ? 1 : 0;
    }
    1;
  };
  unless ( $prefs_ok ) {
    my $e = "$@";
    chomp $e;
    $app->log->error( prefix_dbg() . " prefs snapshot failed: $e" );
    $temp_enabled = undef;
  }

  $tick_cache->{prefs} ||= {};
  $tick_cache->{prefs}{temp_path_enabled} = $temp_enabled;

  for my $ih ( sort keys %$jobs ) {
    my $job = $jobs->{$ih};
    next if ref( $job ) ne 'HASH';

    # ---- LATCH prefs onto the job (single source of truth for this tick) ----
    $job->{temp_path_enabled} = $temp_enabled if defined $temp_enabled;

    # ---- REQUIRED: keep THESE TWO vars alive for the whole loop body ----
    my $enabled         = $job->{temp_path_enabled};    # 1|0|undef
    my $rename_required = ( ref( $job->{rename} ) eq 'HASH' ) ? 1 : 0;
    my $bridged;
    $bridged = ( $enabled && $rename_required ) ? 1 : 0 if defined $enabled;

    # Persist BR for downstream branches + logging (only when knowable)
    $job->{bridged} = $bridged if defined $bridged;

    # ---- ARM plan_br ONCE (first time BR is knowable) ----
    if ( !defined( $job->{plan_br} ) && defined $bridged ) {
      $job->{plan_br}  = $bridged;
      $job->{plan_tpe} = $enabled;    # debug only
    }

    # ---- Detect BR flip (only when both sides are known) ----
    if (    defined( $job->{plan_br} )
         && defined( $bridged )
         && $job->{plan_br} != $bridged )
    {

      unless ( $job->{parachute_injected} ) {
        $job->{parachute_injected} = 1;
        $job->{parachute_reason} =
            "BR flip ($job->{plan_br} -> $bridged) (tpe="
            . ( defined( $enabled ) ? $enabled : 'undef' ) . ")";

        my $si    = $job->{step_idx} // 0;
        my $steps = $job->{steps};

        if ( ref( $steps ) eq 'ARRAY' ) {
          splice @$steps, $si, ( @$steps - $si ),
              {op => 'parachute', tries => 0};
        }
        else {
          $job->{steps}    = [ {op => 'parachute', tries => 0} ];
          $job->{step_idx} = 0;
        }

        $job->{next_ts} = $now;

        $app->log->debug(   prefix_dbg()
                          . "\n######################################"
                          . "\n\tPARACHUTE_NEEDED ih="
                          . short_ih( $ih ) . "\n\t"
                          . ( $job->{parachute_reason} // '' ) );
      }
    }

    # ---- Plan build happens ONLY after prefs are latched ----
    # IMPORTANT: do NOT overwrite plan_br here (plan_br is ARMED ONCE above)

    if (    $job->{plan_needed}
         || ref( $job->{steps} ) ne 'ARRAY'
         || !@{$job->{steps}} )
    {
      $job->{steps}       = _build_steps_add_one( $job );
      $job->{step_idx}    = 0;
      $job->{stage}       = 'queued';
      $job->{plan_needed} = 0;

      $app->log->debug(   prefix_dbg()
                        . "  planned ih: "
                        . short_ih( $ih )
                        . " bridged="
                        . ( defined( $bridged ) ? $bridged : '(undef)' )
                        . " enabled="
                        . ( defined( $enabled ) ? $enabled : '(undef)' )
                        . " rename_required=$rename_required steps="
                        . scalar( @{$job->{steps} || []} ) );
    }

    # -------- needs_user re-init gating (ONLY) --------
    if ( ( $job->{status} // '' ) eq 'needs_user' ) {
      my $reason = $job->{needs_user_reason} // $job->{interactive_reason}
          // '';

      if ( $reason eq 'renamed_root_class' ) {

        # only re-init when temp path is definitively OFF
        if ( defined( $enabled ) && !$enabled ) {
          $job->{status}          = 'queued';
          $job->{stage}           = 'queued';
          $job->{step_idx}        = 0;
          $job->{next_ts}         = $now + 0.5;
          $job->{ts_updated}      = $now;
          $job->{terminal_logged} = 0;
          $job->{steps}           = _build_steps_add_one( $job );

          delete $job->{recheck_requested_ts};
          delete $job->{recheck_last_check0};
          delete $job->{stability_probe_inserted};

          # ---- REQUIRED RESETS (rebase reality) ----
          delete $job->{plan_br};
          delete $job->{plan_tpe};
          delete $job->{parachute_injected};
          delete $job->{parachute_reason};
          $job->{plan_needed} = 1;

          $app->log->debug(   prefix_dbg()
                            . " reinit needs_user ih: "
                            . short_ih( $ih )
                            . " reason=$reason (temp_path_enabled=0)" );
        }
      }
    }

    my $status   = $job->{status}   // '';
    my $stage    = $job->{stage}    // '';
    my $step_idx = $job->{step_idx} // 0;

    # non-terminal jobs: next_ts defaults to now if missing/garbage
    my $next_ts = $job->{next_ts};
    $next_ts = $now if !defined( $next_ts ) || $next_ts !~ /^\d+$/;

    my $wait = $next_ts - $now;

    # Unified per-job-per-tick line: BR + pump_tick + JOB
    $app->log->debug( prefix_dbg() . "  BR:"
                . ( $job->{bridged} ? 1 : 0 )
                . " pump_tick : $sum "
                . " [JOB] ih:"
                . short_ih( $ih )
                . " status: $status stage: $stage step_idx: $step_idx " . " F: "
                . epoch2time( $next_ts )
                . " wait: "
                . sprintf( "% +ds", $wait ) );

    my $is_terminal = ( $status =~ /^(done|failed|needs_user|needs_triage)$/ );
    if ( $is_terminal ) {
      delete $job->{next_ts};

      next if $job->{terminal_logged};
      $job->{terminal_logged} = 1;

      $app->log->debug(
              prefix_dbg()
            . " [JOB] ih: "
            . short_ih( $ih )
            . " status = $status stage = $stage step_idx =
          $step_idx ( terminal ) " );
      next;
    }

    next if $next_ts > $now;

    my $steps = $job->{steps};
    if ( ref( $steps ) ne 'ARRAY' ) {
      $steps = [];
      $job->{steps} = $steps;
    }

    # ---- TERMINAL: OUT OF STEPS -> DONE ----
    if ( $step_idx >= @$steps ) {
      _job_mark_done( $app, $job, $now );
      next;
    }

    if ( @$steps ) {
      my $remain = @$steps - $step_idx;
      $remain = 0 if $remain < 0;

      my @ops =
          map {
        ( ref( $_ ) eq 'HASH' )
            ? (
          $_->{op} // "
      ? " )
            : "
            ? "
          } @$steps[ $step_idx .. $#$steps ];

      my $ops;
      if ( @ops <= 4 ) {
        $ops = join( ", ", @ops );
      }
      else {
        $ops = join( ", ", @ops[ 0 .. 2 ] ) . ", ..., " . $ops[-1];
      }

      $app->log->debug(   prefix_dbg()
                        . "  [PLAN] ih: "
                        . short_ih( $ih )
                        . " steps_remaining = $remain ops = [$ops] " );
    }

    my $step = $steps->[$step_idx];
    if ( ref( $step ) ne 'HASH' ) {
      _job_mark_failed( $app, $job, $now,
                        " bad step at idx = $step_idx ( not a HASH ) " );
      next;
    }

    $step->{tries} = 0 if !defined $step->{tries};

    my $op = $step->{op} // '';
    $job->{stage} = $op;

    my ( $result, $why, $hint ) =
        _run_step( $app, $qbt, $job, $step, $tick_cache );

    $app->log->debug(   prefix_dbg()
                      . "  step ih : "
                      . short_ih( $ih )
                      . " op = $op result = $result why = "
                      . ( $why // '' ) );

    if ( $result eq 'advance' ) {
      _job_advance_step( $job, $now );
      next;
    }

    if ( $result eq 'retry' ) {
      my $rr = _job_retry_step( $app, $job, $step, $now, $why );

      if ( defined( $rr ) && $rr eq 'needs_user' ) {
        if ( $bridged ) {
          _bless_paths( $app, $qbt, $job, $tick_cache );
          _qbt_remove_torrent( $app, $qbt, $ih, delete_files => 0 );
        }
        next;
      }
      next;
    }

    if ( $result eq 'interactive' ) {
      if ( $bridged ) {
        _bless_paths( $app, $qbt, $job, $tick_cache );
        _qbt_remove_torrent( $app, $qbt, $ih, delete_files => 0 );
      }

      my $reason = $why  || 'unknown';
      my $detail = $hint || '';
      _job_mark_needs_user( $app, $job, $now, $reason, $why, $detail );
      next;
    }

    if ( $result eq 'renamed_root' ) {
      if ( $bridged ) {
        _bless_paths( $app, $qbt, $job, $tick_cache );
        _qbt_remove_torrent( $app, $qbt, $ih, delete_files => 0 );
      }
      _job_mark_renamed_root( $app, $job, $now, $why, $hint );
      next;
    }

    # edge-case catcher
    my $is_rename_symlink_class =
        ( ref( $job->{rename} ) eq 'HASH' ) || ( $job->{symlink_used} );
    if ( $is_rename_symlink_class ) {
      _job_mark_needs_triage(
                              $app,
                              $job,
                              $now,
                              'unexpected_result',
                              ( $why || " failed( result = $result ) " ),
                              " op = "
                                  . ( $job->{stage} // '' )
                                  . " step_idx = "
                                  . ( $job->{step_idx} // 0 ) );
      next;
    }

    _job_mark_failed(
      $app, $job, $now, $why
          || " failed( result = $result )
          " );
  }

  return;
}

# ------------------------------
# Job state helpers
# ------------------------------

sub _job_mark_done {
  my ( $app, $job, $now ) = @_;

  $job->{status}     = 'done';
  $job->{ts_updated} = $now;
  delete $job->{next_ts};

  # --- clear stale needs_user classifier(s) after a successful full run ---
  if ( my $r = $job->{needs_user_reason} ) {

    if ( ref( $job->{needs_user_seen} ) eq 'HASH' ) {
      delete $job->{needs_user_seen}{$r};

      # if empty now, drop the whole key
      delete $job->{needs_user_seen} unless keys %{$job->{needs_user_seen}};
    }

    delete $job->{needs_user_reason};  # optional, if “done” shouldn’t retain it
    delete $job->{needs_user_why};     # optional
    delete $job->{needs_user_hint};    # optional
    delete $job->{needs_user_once};    # optional
  }

  return 1;
}

# $job->{next_ts} = $now + 9999999;

sub _job_mark_failed {
  my ( $app, $job, $now, $why ) = @_;
  $job->{status}       = 'failed';
  $job->{last_error}   = $why || 'failed';
  $job->{failed_ts}    = $now;
  $job->{failed_stage} = $job->{stage} // '';
  $job->{ts_updated}   = $now;

  delete $job->{next_ts};
  _cleanup_symlink_if_any( $app, $job );
  return 1;
}

sub _job_mark_needs_user {
  my ( $app, $job, $now, $reason, $why, $hint ) = @_;
  my $ih = $job->{ih} // '';
  return unless $ih =~ /^[0-9a-f]{40}$/;

  $reason ||= 'unknown';
  $why    ||= '';
  $hint   ||= '';

  # If we already landed here once for this same reason, escalate to triage.
  if ( $job->{needs_user_seen} && $job->{needs_user_seen}{$reason} ) {
    return _job_mark_needs_triage(
      $app, $job, $now, " repeat_needs_user
          : $reason ",
      $why, $hint );
  }

  $job->{needs_user_reason} = $reason;
  $job->{needs_user_once}   = 1;
  $job->{needs_user_why}    = $why;
  $job->{needs_user_hint}   = $hint;
  $job->{needs_user_ts}     = $now;
  $job->{needs_user_seen} ||= {};
  $job->{needs_user_seen}{$reason} = 1;
  $job->{status}                   = 'needs_user';
  $job->{ts_updated}               = $now;

  delete $job->{next_ts};

  _cleanup_symlink_if_any( $app, $job );

  my $g = _gui_ref( $app );
  $g->{$ih} = {
               ih       => $ih,
               ts       => $now,
               reason   => $reason,
               why      => $why,
               hint     => $hint,
               stage    => ( $job->{stage}    // '' ),
               step_idx => ( $job->{step_idx} // 0 ),};

  $app->log->debug(
          prefix_dbg()
        . " needs_user ih : "
        . short_ih( $ih )
        . " reason : $reason stage : $g->{$ih}{stage} why : $why
          " );

  return 1;
}

sub _job_mark_needs_triage {
  my ( $app, $job, $now, $reason, $why, $hint ) = @_;
  my $ih = $job->{ih} // '';
  return unless $ih =~ /^[0-9a-f]{40}$/;

  $reason ||= 'unknown';
  $why    ||= '';
  $hint   ||= '';

  $job->{status}     = 'needs_triage';
  $job->{ts_updated} = $now;
  delete $job->{next_ts};

  $job->{triage_reason} = $reason;
  $job->{triage_why}    = $why;
  $job->{triage_hint}   = $hint;
  $job->{triage_ts}     = $now;

  _cleanup_symlink_if_any( $app, $job );

  my $t = _triage_ref( $app );
  $t->{$ih} = {
               ih       => $ih,
               ts       => $now,
               reason   => $reason,
               why      => $why,
               hint     => $hint,
               stage    => ( $job->{stage}    // '' ),
               step_idx => ( $job->{step_idx} // 0 ),};

  $app->log->error(
          " : "
        . __LINE__
        . " needs_triage ih : "
        . short_ih( $ih )
        . " reason : $reason stage : $t->{$ih}{stage} why : $why
          " );

  return 1;
}

sub _job_mark_renamed_root {
  my ( $app, $job, $now, $why, $hint ) = @_;
  my $ih = $job->{ih} // '';
  return unless $ih =~ /^[0-9a-f]{40}$/;

  $job->{status}     = 'renamed_root';
  $job->{last_error} = $why || 'renamed_root';
  $job->{ts_updated} = $now;

  delete $job->{next_ts};
  _cleanup_symlink_if_any( $app, $job );

  my $r = _renamed_root_ref( $app );
  $r->{$ih} = {
               ih       => $ih,
               ts       => $now,
               reason   => ( $why  || '' ),
               hint     => ( $hint || '' ),
               stage    => ( $job->{stage}    // '' ),
               step_idx => ( $job->{step_idx} // 0 ),};

  $app->log->debug(   prefix_dbg()
                    . " renamed_root ih : "
                    . short_ih( $ih )
                    . " reason : $r->{$ih}{reason} " );
  return 1;
}

# ------------------------------
# Symlink helpers
# ------------------------------

sub _cleanup_symlink_if_any {
  my ( $app, $job ) = @_;
  my $link = $job->{symlink_path} // '';
  return 0 unless length $link;
  if ( -l $link ) {
    my $ok = unlink $link;
    $app->log->debug( prefix_dbg() . "  cleanup symlink : $link ok : $ok " );
  }
  else {
    $app->log->debug(
      prefix_dbg() . " symlink cleanup skipped( not a symlink ) link : $link
          " );
  }

  delete $job->{symlink_path};
  delete $job->{symlink_target};
  return 1;
}

sub _ensure_symlink_for_recheck {
  my ( $app, $job, %args ) = @_;
  my $target = $args{target} // '';
  my $link   = $args{link}   // '';

  my $rename_required = ( ref( $job->{rename} ) eq 'HASH' ) ? 1 : 0;

  # Single source of truth: pump_tick latches this every tick.
  my $bridged;
  if ( exists $job->{bridged} ) {
    $bridged = $job->{bridged} ? 1 : 0;
  }
  else {
    my ( $pkg, $file, $line, $sub ) = caller( 1 );    # 1 = immediate caller
    $app->log->debug( " #######################################################"
           . "\nlegacy fallback caller: sub=$sub file=$file line=$line pkg=$pkg"
    );
    $app->log->debug( prefix_dbg()
             . "\n##########################################################"
             . "\nlegacy fallback was used but should be unnecessary."
             . "\nnot all callers are latching properly and should be fixed!"
             . "\n##########################################################" );
    my $enabled = $job->{temp_path_enabled} ? 1 : 0;

    my $rename_required = ( ref( $job->{rename} ) eq 'HASH' ) ? 1 : 0;
    $bridged = ( defined( $enabled ) && $enabled && $rename_required ) ? 1 : 0;
  }

  #   $app->log->debug(   basename( __FILE__ ) . ":"
  #                     . __LINE__
  #                     . " symlink_gate ih="
  #                     . short_ih( $ih )
  #                     . " bridged= $bridged enabled="
  #                     . ( defined( $enabled ) ? $enabled : '(undef)' )
  #                     . " rename_required=$rename_required" );
  return ( 1, "symlink not permitted ($bridged=0)" ) unless $bridged;
  return ( 0, " missing target " )                   unless length $target;
  return ( 0, " missing link " )                     unless length $link;

  # target must exist (file OR dir)
  return (
    0, " target does not exist
    : $target "
  ) if !-e $target;

  # link parent must exist
  my $parent = dirname( $link );
  return ( 0, " link parent dir missing : $parent " ) if !-d $parent;

  # helper: compare link destination robustly (handles relative readlink())
  my $same_dest = sub {
    my ( $cur, $want, $link_path ) = @_;
    return 0 unless defined $cur && length $cur;

    # If readlink returned relative, resolve relative to link's directory
    my $cur_abs =
        ( $cur =~ m{^/} ) ? $cur : ( dirname( $link_path ) . "/$cur" );

    # abs_path can return undef in odd cases; fall back to string compare
    my $a = abs_path( $cur_abs );
    my $b = abs_path( $want );

    return 1 if defined( $a ) && defined( $b ) && $a eq $b;
    return 1 if $cur eq $want;    # fallback (covers exact match)
    return 0;
  };

  # If link exists and is a symlink...
  if ( -l $link ) {
    my $cur = readlink( $link );

    if ( $same_dest->( $cur, $target, $link ) ) {

      # record on job so cleanup can happen later
      $job->{symlink_path}   = $link;
      $job->{symlink_target} = $target;
      $job->{symlink_used}   = 1;
      return ( 1, " symlink already ok " );
    }

    # wrong symlink -> replace
    unlink $link
        or return ( 0, " failed to unlink existing symlink $link ( $! ) " );

    if ( symlink( $target, $link ) ) {
      $job->{symlink_path}   = $link;
      $job->{symlink_target} = $target;
      $job->{symlink_used}   = 1;
      $app->log->debug(
        prefix_dbg() . "  symlink replaced link =
        $link->$target " );
      return ( 1, " symlink replaced " );
    }

    return (
      0, " failed to replace symlink link = $link->target =
        $target ( $! ) " );
  }

  # If it exists but isn't a symlink, hard-stop
  if ( -e $link ) {
    return ( 0, " link path exists and is not a symlink : $link " );
  }

  # Create new symlink
  if ( symlink( $target, $link ) ) {
    $job->{symlink_path}   = $link;
    $job->{symlink_target} = $target;
    $job->{symlink_used}   = 1;
    $app->log->debug(
                     prefix_dbg() . "  symlink created link: $link->$target " );
    return ( 1, " symlink created " );
  }

  return (
    0, " failed to create symlink link = $link->target =
        $target ( $! ) " );
}

sub _job_retry_step {
  my ( $app, $job, $step, $now, $why ) = @_;

  my $op = $step->{op} // '';
  $job->{status}     = 'running';
  $job->{ts_updated} = $now;
  $job->{last_error} = $why // '';

# SPECIAL: probe_checking should sleep until probe_after_ts (no backoff, no tries)
  if ( $op eq 'probe_checking' && $step->{probe_after_ts} ) {
    my $ts = $step->{probe_after_ts};
    $job->{next_ts} = $ts if $ts > $now;
    $app->log->debug(   prefix_dbg()
                      . "  probe_checking sleep_until: "
                      . epoch2time( $job->{next_ts} ) );
    return 1;
  }

  $step->{tries} = 0 if !defined $step->{tries};
  $step->{tries}++;

  # Backoff schedule by op
  my %sched = (
                wait_visible     => [ 1, 2, 3,  5,  8, 13 ],
                rename_root      => [ 2, 5, 10, 20, 30 ],
                confirm_rename   => [ 2, 5, 10 ],
                recheck          => [ 2, 5, 10, 20, 30 ],
                confirm_checking => [ 5, 30 ],
                probe_checking   => [ 5, 5, 5 ], );
  my $arr = $sched{$op} || [ 5, 10, 20, 30 ];

  # If we exceeded the schedule, bounce to needs_user
  if ( $step->{tries} > scalar( @$arr ) ) {
    my $hint = '';
    if ( $op eq 'recheck' || $op eq 'confirm_checking' ) {
      my $link = $job->{symlink_path}   // '';
      my $tgt  = $job->{symlink_target} // '';
      $hint =
          ( length( $link ) && length( $tgt ) )
          ? " symlink was : $link->$tgt "
          : '';
    }
    _job_mark_needs_user( $app, $job, $now,
                          ( $why || " blocked after retries( op: $op ) " ),
                          $hint, " No reason provided " );
    return 'needs_user';
  }

  my $delay = $arr->[ $step->{tries} - 1 ];
  $job->{next_ts} = $now + $delay;

  $app->log->debug(   prefix_dbg()
                    . " retry ih: "
                    . short_ih( $job->{ih} // '' )
                    . " op: $op tries: $step->{tries} next_in: ${delay} s why: "
                    . ( $why // '' ) );

  return 1;
}

sub _job_advance_step {
  my ( $job, $now ) = @_;

  my $steps    = $job->{steps}    || [];
  my $step_idx = $job->{step_idx} || 0;

  # what step did we just finish?
  my $cur_step =
      ( ref( $steps->[$step_idx] ) eq 'HASH' ) ? $steps->[$step_idx] : {};
  my $cur_op = $cur_step->{op} // '';

  # advance index
  $job->{step_idx}   = $step_idx + 1;
  $job->{status}     = 'running';
  $job->{last_error} = '';
  $job->{ts_updated} = $now;
  $job->{next_ts}    = $now;

  #   # ---- dynamic injection: stability probe after remove_symlink ----
  #   # Only when a symlink was used, and only once.
  #   if (    $cur_op eq 'remove_symlink'
  #        && $job->{symlink_used}
  #        && !$job->{stability_probe_inserted} )
  #   {
  #     splice(
  #       @$steps,
  #       $job->{step_idx},
  #       0,
  #       {
  #        op             => 'remove_symlink',
  #        tries          => 0,
  #        probe_after_ts => $now + 10,    # run this probe ~60s later
  #       } );
  #
  #     $job->{stability_probe_inserted} = 1;
  #
  #     # schedule probe
  #     $job->{next_ts} = $now + 10;
  #   }

  if ( $job->{step_idx} >= @$steps ) {
    $job->{stage} = '';
  }
  else {
    my $next_step = $steps->[ $job->{step_idx} ];
    my $next_op =
        ( ref( $next_step ) eq 'HASH' ) ? ( $next_step->{op} // '' ) : '';
    $job->{stage}       = $next_op;
    $next_step->{tries} = 0
        if ref( $next_step ) eq 'HASH' && !defined $next_step->{tries};
  }

  return 1;
}

# ------------------------------
# qBt  helpers
# ------------------------------

sub _qbt_torrents_info_one_cached {
  my ( $qbt, $tick_cache, $ih ) = @_;
  $tick_cache ||= {};
  $tick_cache->{torrents_info_one} ||= {};

  # capture last qBt API error for callers (e.g. wait_visible)
  delete $tick_cache->{qbt_last_err};

  return $tick_cache->{torrents_info_one}{$ih}
      if exists $tick_cache->{torrents_info_one}{$ih};

  my $t;

  # Preferred: a single-torrent endpoint if your QBT wrapper has it.
  $t = eval { $qbt->api_torrents_info_one( $ih ) };
  if ( $@ ) {
    my $e = "$@";
    chomp $e;
    $tick_cache->{qbt_last_err} = "api_torrents_info_one died: $e";
  }
  if ( ref( $t ) eq 'HASH' ) {
    $tick_cache->{torrents_info_one}{$ih} = $t;
    return $t;
  }

  # Fallback: list endpoint + grep (common in wrappers)
  my $arr = eval { $qbt->api_torrents_info() };
  if ( $@ ) {
    my $e = "$@";
    chomp $e;
    $tick_cache->{qbt_last_err} ||= "api_torrents_info died: $e";
  }

  # If your wrapper returns {ok=>0,...} on failure, surface that as an error.
  if ( ref( $arr ) eq 'HASH' && exists $arr->{ok} && !$arr->{ok} ) {
    my $code = $arr->{code} // 0;
    my $body = $arr->{body} // '';
    $tick_cache->{qbt_last_err} ||=
        "api_torrents_info not ok: http: $code body: [$body]";
    $tick_cache->{torrents_info_one}{$ih} = undef;
    return undef;
  }
  if ( ref( $arr ) eq 'ARRAY' ) {
    for my $x ( @$arr ) {
      next unless ref( $x ) eq 'HASH';
      my $h = $x->{hash} // '';
      next unless $h eq $ih;
      $tick_cache->{torrents_info_one}{$ih} = $x;
      return $x;
    }
  }

  $tick_cache->{torrents_info_one}{$ih} = undef;
  return undef;
}

sub _qbt_remove_torrent {
  my ( $app, $qbt, $ih, %args ) = @_;
  my $delete_files = $args{delete_files} ? 1 : 0;    # default: keep files

  #   my $res = eval { $qbt->api_torrents_delete( $ih, $delete_files ); };
  my $res = eval { $qbt->api_torrents_stop( $ih, $delete_files ); };
  if ( $@ ) {
    my $e = "$@";
    chomp $e;
    $app->log->debug(
        prefix_dbg() . " qbt delete threw ih: " . short_ih( $ih ) . " err=$e" );
    return ( 0, $e );
  }

  if ( ref( $res ) eq 'HASH' && $res->{ok} ) {
    $app->log->debug(   prefix_dbg()
                      . "  qbt delete ok ih: "
                      . short_ih( $ih )
                      . " delete_files: $delete_files" );
    return ( 1, 'ok' );
  }

  my $code = ( ref( $res ) eq 'HASH' ) ? ( $res->{code} // 0 )  : 0;
  my $body = ( ref( $res ) eq 'HASH' ) ? ( $res->{body} // '' ) : '';
  $app->log->debug(   prefix_dbg()
                    . "  qbt delete not ok ih: "
                    . short_ih( $ih )
                    . " http: $code body: [$body]" );
  return ( 0, "http: $code body: [$body]" );
}

sub _qbt_prefs_cached {
  my ( $qbt, $tick_cache ) = @_;
  $tick_cache ||= {};
  return $tick_cache->{app_prefs} if exists $tick_cache->{app_prefs};

  my $p = eval { $qbt->api_qbt_preferences() };
  $tick_cache->{app_prefs} = ( ref( $p ) eq 'HASH' ) ? $p : undef;
  return $tick_cache->{app_prefs};
}

# ------------------------------
# Step runner
# ------------------------------

sub _inject_parachute {
  my ( $job, %opt ) = @_;
  my $reason = $opt{reason} // 'unknown';

  return 0 unless ref( $job ) eq 'HASH';
  return 0 if $job->{parachute_injected};

  $job->{parachute_injected} = 1;
  $job->{parachute_reason}   = $reason;

  $job->{plan} ||= {};
  $job->{plan}{ops}    = [ {op => 'parachute', tries => 0} ];
  $job->{plan}{op_idx} = 0;

  # run ASAP
  $job->{next_ts} = time;

  return 1;
}

sub _run_step {
  my ( $app, $qbt, $job, $step, $tick_cache ) = @_;
  $tick_cache ||= {};
  my $op = $step->{op} // '';

  my $ih = $job->{ih} // '';
  return ( 'fail', "bad ih in job (missing/invalid)" )
      unless $ih =~ /^[0-9a-f]{40}$/;

  if ( $op eq 'parachute' ) {

    #     $app->log->debug(   basename( __FILE__ ) . ":"
    #                       . __LINE__
    #                       . "\n##########################################"
    #                       . "\n\tPARACHUTE DEPLOYED"
    #                       . "\n##########################################" );

    # If it’s visible, bless once (authoritative snapshot) using the same t0
    my $t0 = _qbt_torrents_info_one_cached( $qbt, $tick_cache, $ih );
    if ( ref( $t0 ) eq 'HASH' ) {
      _bless_paths( $app, $qbt, $job, $tick_cache, t0 => $t0 );
    }

    # inside _run_step
    if ( $op eq 'parachute' ) {
      $app->log->debug(   prefix_dbg()
                        . "\n##########################################"
                        . "\n\tPARACHUTE DEPLOYED"
                        . "\n##########################################"
                        . "\n\tih="
                        . short_ih( $ih ) . " "
                        . ( $job->{parachute_reason} // '' ) );

      # Bless if visible (non-destructive snapshot)
      my $t0 = _qbt_torrents_info_one_cached( $qbt, $tick_cache, $ih );
      if ( ref( $t0 ) eq 'HASH' ) {
        _bless_paths( $app, $qbt, $job, $tick_cache );
      }

      # After parachute, we must NOT "advance -> done".
      # We re-base the job and force a re-plan for next tick.

      # clear run-state that would prevent a clean re-run
      delete $job->{recheck_requested_ts};
      delete $job->{recheck_last_check0};
      delete $job->{stability_probe_inserted};
      delete $job->{symlink_used};
      delete $job->{symlink_path};
      delete $job->{symlink_target};

      # allow future parachutes (if it flips again later)
      delete $job->{parachute_injected};
      delete $job->{parachute_reason};

      # IMPORTANT: re-arm plan baseline at new reality
      delete $job->{plan_br};
      delete $job->{plan_tpe};

      # force rebuild steps
      $job->{plan_needed} = 1;
      $job->{steps}       = [];
      $job->{step_idx}    = 0;
      $job->{stage}       = 'queued';
      $job->{status}      = 'queued';
      $job->{next_ts}     = time;

      return ( 'retry', 'parachute: rebased + replanning next tick' );
    }

    my $bridged = $job->{bridged}       ? 1 : 0;    # LATCHED at plan-build time
    my $blessed = $job->{paths_blessed} ? 1 : 0;

   # Non-bridged jobs: prefs flip is irrelevant; stop cleanly (init zen handoff)
    return ( 'advance', 'parachute: not bridged, safe stop' ) unless $bridged;

    # Bridged jobs: we must abort this run and route to interactive triage
    # If we managed to bless, great; otherwise still abort (but note unblessed)
    if ( $blessed ) {
      return ( 'interactive', 'renamed_root_class',
               'parachute: bridged+blessed; bail' );
    }

    return ( 'interactive', 'unblessed_abort',
             'parachute: bridged+UNBLESSED; bail' );
  }

  if ( $op eq 'confirm_checking' ) {
    my $req_ts  = $job->{recheck_requested_ts} // 0;
    my $base_lc = $job->{recheck_last_check0};

    my $t0 = _qbt_torrents_info_one_cached( $qbt, $tick_cache, $ih );
    return ( 'retry', "confirm_checking: not visible yet" )
        unless ref( $t0 ) eq 'HASH';

    my $state      = $t0->{state} // '';
    my $progress   = exists $t0->{progress}   ? $t0->{progress}   : undef;
    my $last_check = exists $t0->{last_check} ? $t0->{last_check} : undef;

    # SUCCESS SIGNAL #1: checking-ish state
    if ( $state =~
/^(checking|checkingDL|checkingResumeData|queuedForChecking|queuedForCheck|allocating)/i
        )
    {
      if ( !$job->{paths_blessed} ) {
        _bless_paths( $app, $qbt, $job, $tick_cache );    # non-destructive
        $job->{paths_blessed} = 1;
      }
      return ( 'advance', "confirm_checking: now checking (state: $state)" );
    }

    # SUCCESS SIGNAL #2: last_check moved
    if ( defined $last_check ) {
      my $lc = $last_check;
      $lc = 0 if $lc < 0;

      if ( defined $base_lc ) {
        my $b = $base_lc;
        $b = 0 if $b < 0;
        if ( $lc > $b ) {
          if ( !$job->{paths_blessed} ) {
            _bless_paths( $app, $qbt, $job, $tick_cache );
            $job->{paths_blessed} = 1;
          }

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

    return (
             'retry',
             "confirm_checking: not reflected yet (state: $state"
                 . " last_check="
                 . ( defined( $last_check ) ? $last_check : '(undef)' )
                 . " progress="
                 . ( defined( $progress ) ? $progress : '(undef)' ) . ")" );
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

  if ( $op eq 'confirm_not_checking' ) {
    my $t0 = _qbt_torrents_info_one_cached( $qbt, $tick_cache, $ih );
    return ( 'retry', "confirm_not_checking: not visible yet" )
        unless ref( $t0 ) eq 'HASH';

    my $state = $t0->{state} // '';
    return ( 'retry',
             "confirm_not_checking: unexpectedly checking (state: $state)" )
        if $state =~ /checking/i;

    return ( 'advance', "confirm_not_checking: not checking (state: $state)" );
  }

  if ( $op eq 'delete_torrent' ) {

    return ( 'advance', 'delete_torrent: already requested' )
        if $step->{delete_requested};

# HARD LOCK: never delete data
#     my $res = $qbt->api_torrents_delete( $ih, 0 );    # 0 => deleteFiles=false
    my $res = $qbt->api_torrents_stop( $ih, 0 );    # 0 => deleteFiles=false

    if ( ref( $res ) eq 'HASH' && $res->{ok} ) {
      $step->{delete_requested} = 1;
      return ( 'advance', 'delete_torrent: removed from qBt (data kept)' );
    }

    my $code = ( ref( $res ) eq 'HASH' ) ? ( $res->{code} // 0 )  : 0;
    my $body = ( ref( $res ) eq 'HASH' ) ? ( $res->{body} // '' ) : '';

    return ( 'retry', "delete_torrent: failed http: $code body: [$body]" );
  }

  if ( $op eq 'interactive' ) {
    return ( 'interactive', 'renamed_root_class',
             'rename/symlink class confirmed' );
  }

  if ( $op eq 'probe_checking' ) {
    $step->{probe_after_ts} ||= time + 10;
    return ( 'retry', 'probe_checking: waiting' )
        if time < $step->{probe_after_ts};

    my $t0 = _qbt_torrents_info_one_cached( $qbt, $tick_cache, $ih );
    return ( 'retry', "probe_checking: not visible yet" )
        unless ref( $t0 ) eq 'HASH';

    my $state = $t0->{state} // '';

    # expected: it is NOT checking anymore
    if ( $state =~ /checking/i ) {
      return ( 'retry', "probe_checking: still checking (state: $state)" );
    }

    return ( 'advance', "probe_checking: died as expected (state: $state)" );
  }

  if ( $op eq 'recheck' ) {

    return ( 'advance', $op . ': already requested' )
        if $step->{recheck_requested};

    my $t0 = _qbt_torrents_info_one_cached( $qbt, $tick_cache, $ih );
    return ( 'retry', "recheck: not visible yet" ) unless ref( $t0 ) eq 'HASH';

    $app->log->debug(   prefix_dbg()
                      . " $op ih: "
                      . short_ih( $ih )
                      . " state: "
                      . ( $t0->{state} // '' )
                      . " save_path: "
                      . ( $t0->{save_path} // '' )
                      . " download_path="
                      . ( $t0->{download_path} // '' ) );

    # Snapshot last_check baseline for confirm_checking
    if ( exists $t0->{last_check} ) {
      $job->{recheck_last_check0} = $t0->{last_check};
    }

    my $save_path     = $t0->{save_path}     // '';
    my $download_path = $t0->{download_path} // '';

# --- Decide what "root" qBt expects (folder for multi, filename for single) ---
    my $root = '';

    # 1) Prefer rename-based root (your multi-torrent rename case)
    if ( ref( $job->{rename} ) eq 'HASH' ) {
      $root = $job->{rename}{drivespace_top_lvl} // '';
      $root ||= $job->{rename}{torrent_top_lvl} // '';
    }

    # 2) Fallback: derive from torrents/files
    if ( !length( $root ) ) {
      my $arr = eval { $qbt->api_torrents_files( $ih ) };
      if ( ref( $arr ) eq 'ARRAY' && @$arr ) {
        my $p = '';
        for my $f ( @$arr ) {
          next unless ref( $f ) eq 'HASH';
          $p = $f->{name} // $f->{path} // '';
          last if length $p;
        }
        if ( length $p ) {
          ( $root ) = split m{/}, $p, 2;    # multi: Root/file; single: file
          $root ||= '';
        }
      }
    }

    # --- SYMLINK BRIDGE (only if qBt's download_path != save_path) ---
    my $enabled         = $job->{temp_path_enabled}           ? 1 : 0; # latched
    my $rename_required = ( ref( $job->{rename} ) eq 'HASH' ) ? 1 : 0;
    my $bridged         = ( $enabled && $rename_required )    ? 1 : 0;
    my $prev            = $job->{bridged_prev};
    $job->{bridged_prev} = $bridged;

    if ( defined( $prev ) && $prev != $bridged ) {
      _inject_parachute( $job, reason => "bridged_flip $prev->$bridged" );
    }

    if (    length( $save_path )
         && length( $download_path )
         && length( $root )
         && $download_path ne $save_path )
    {
      my $target = "$save_path/$root";
      my $link   = "$download_path/$root";

      if ( $target eq $link ) {
        $app->log->debug( prefix_dbg()
                      . " recheck: symlink skipped (target==link) path=$link" );
      }
      else {
        my ( $ok, $why ) =
            _ensure_symlink_for_recheck(
                                         $app, $job,
                                         target => $target,
                                         link   => $link, );

        unless ( $ok ) {
          return ( 'retry', "recheck: symlink prep failed: $why" );
        }

        $app->log->debug( prefix_dbg() . " recheck: symlink prep ok: $why " );

#                 . " recheck: symlink prep ok: $why link=$link target=$target" );
      }
    }

    my $r = $qbt->api_torrents_recheck( $ih );
    if ( ref( $r ) ne 'HASH' || !$r->{ok} ) {
      my $code = ( ref( $r ) eq 'HASH' ) ? ( $r->{code} // 0 )  : 0;
      my $body = ( ref( $r ) eq 'HASH' ) ? ( $r->{body} // '' ) : '';
      return ( 'retry', "recheck: recheck not ok: http: $code body: [$body]" );
    }

    $job->{recheck_requested_ts} = time;
    $step->{recheck_requested}   = 1;
    delete $tick_cache->{torrents_info_one}{$ih};

    my $t1 = _qbt_torrents_info_one_cached( $qbt, $tick_cache, $ih );
    if ( ref( $t1 ) eq 'HASH' ) {
      $app->log->debug(   prefix_dbg()
                        . " post-recheck ih: "
                        . short_ih( $ih )
                        . " state: "
                        . ( $t1->{state} // '' )
                        . " save_path: "
                        . ( $t1->{save_path} // '' )
                        . " download_path="
                        . ( $t1->{download_path} // '' ) );
    }

    return ( 'advance', 'recheck: requested ok' );
  }

  if ( $op eq 'recheck_again' ) {
    my $r = $qbt->api_torrents_recheck( $ih );
    return ( 'retry', $op . ': recheck not ok' )
        unless ref( $r ) eq 'HASH' && $r->{ok};

    $step->{recheck_again_ts} = time;
    delete $tick_cache->{torrents_info_one}{$ih};
    return ( 'advance', $op . ': requested' );
  }

  if ( $op eq 'remove_symlink' ) {
    _cleanup_symlink_if_any( $app, $job );
    return ( 'advance', $op . ': removed' );
  }

  if ( $op eq 'rename_root' ) {
    my $rr = $job->{rename};
    return ( 'advance', $op . ': no rename requested' )
        unless ref( $rr ) eq 'HASH';

    my $oldPath = $rr->{torrent_top_lvl}    // '';
    my $newPath = $rr->{drivespace_top_lvl} // '';

    unless ( length( $oldPath ) && length( $newPath ) ) {
      return ( 'advance', $op . ': missing paths' );
    }
    if ( $oldPath eq $newPath ) { return ( 'advance', $op . ': old==new' ); }

    if ( $step->{rename_requested} ) {
      return ( 'advance', $op . ': already requested' );
    }

    my $res = $qbt->api_torrents_renameFolder( $ih, $oldPath, $newPath );

    if ( ref( $res ) eq 'HASH' && $res->{ok} ) {
      $step->{rename_requested} = 1;
      return ( 'advance', $op . ': requested ok' );
    }

    my $code = ( ref( $res ) eq 'HASH' ) ? ( $res->{code} // 0 )  : 0;
    my $body = ( ref( $res ) eq 'HASH' ) ? ( $res->{body} // '' ) : '';

    if ( $code == 409 && $body =~ /already exists/i ) {
      $step->{rename_requested} = 1;
      return ( 'advance', "rename_root: 409 already-exists; polling confirm" );
    }

    return ( 'retry',
             "rename_root: renameFolder not ok: http: $code body: [$body]" );
  }

  if ( $op eq 'wait_visible' ) {
    my $t = _qbt_torrents_info_one_cached( $qbt, $tick_cache, $ih );

    if ( ref( $t ) eq 'HASH' && ( $t->{hash} // '' ) =~ /^[0-9a-f]{40}$/ ) {
      return ( 'advance', $op . ': now visible' );
    }

    if (    exists $tick_cache->{qbt_last_err}
         && defined $tick_cache->{qbt_last_err}
         && length $tick_cache->{qbt_last_err} )
    {
      return ( 'retry',
               $op . ': qbt api error: ' . $tick_cache->{qbt_last_err} );
    }

    return ( 'retry', $op . ': not visible yet' );
  }

#   if ( $op eq 'probe_recheck_60s' ) {
#     my $ts = $job->{recheck_probe_ts} // 0;
#     return ( 'retry', 'probe: waiting for 60s mark' ) if $ts && time < $ts;
#
#     my $t = _qbt_torrents_info_one_cached( $qbt, $tick_cache, $ih );
#     return ( 'retry', 'probe: not visible yet' ) unless ref( $t ) eq 'HASH';
#
#     my $state      = $t->{state} // '';
#     my $progress   = exists $t->{progress}   ? $t->{progress}   : undef;
#     my $last_check = exists $t->{last_check} ? $t->{last_check} : undef;
#
#     # If it's still checking-ish OR it made forward progress, it "survived"
#     if ( $state =~
# /^(checking|checkingResumeData|queuedForChecking|queuedForCheck|allocating)/i
#         )
#     {
#       return ( 'advance', "probe: still checking (state: $state)" );
#     }
#
# # Some builds flip states fast; last_check advancing is also a good survival signal
#     my $base = $job->{recheck_last_check0};
#     if ( defined $last_check && defined $base && $last_check > $base ) {
#       return ( 'advance', "probe: last_check advanced ($base -> $last_check)" );
#     }
#
#   }

  return ( 'fail', "unknown op=$op" );
}

1;

=pod

#
# sub _maybe_reinit_needs_user {
#   my ( $app, $qbt, $tick_cache, $job, $now ) = @_;
#   $app->log->debug(
#
# #                   basename(__FILE__) . ":" . __LINE__ . "  maybe_reinit_needs_user ih = "
# #                . ( $job->{ih} // '' )
# #                . " status = "
# #                . ( $job->{status} // '' )
# #                . " reason = "
# #                . (
#     $job->{needs_user_reason} // $job->{interactive_reason} // '(none)'
#
#         #                )
#   );
#
#   return 0 unless ref( $job ) eq 'HASH';
#
#   return 0 unless ( $job->{status} // '' ) eq 'needs_user';
#
#   # Only re-init for the specific interactive reason you named
#   my $reason = $job->{needs_user_reason} // $job->{interactive_reason} // '';
#   return 0 unless $reason eq 'renamed_root_class';
#
#   my $enabled = $app->defaults->{qbt}{prefs}{temp_path_enabled}; = _qbt_temp_path_enabled_cached( $qbt, $tick_cache );
#
#   # If we can't read prefs, or it's still enabled -> do nothing, stay needs_user
#   return 0 if !defined( $enabled ) || $enabled;
#
#   # OK: condition fixed -> re-init job
#   $job->{status}          = 'queued';
#   $job->{stage}           = 'queued';
#   $job->{step_idx}        = 0;
#   $job->{steps}           = _build_steps_add_one(  $job );
#   $job->{next_ts}         = $now + 0.5;
#   $job->{ts_updated}      = $now;
#   $job->{terminal_logged} = 0;    # allow logs again now that it’s active
#
#   # clear per-run latches
#   delete $job->{recheck_requested_ts};
#   delete $job->{recheck_last_check0};
#   delete $job->{stability_probe_inserted};
#
#   return 1;
# }
#
# sub _temp_path_enabled {
#   my ( $qbt, $tick_cache ) = @_;
#   my $p = _qbt_prefs_cached( $qbt, $tick_cache );
#   return 0 unless ref( $p ) eq 'HASH';
#   return $p->{temp_path_enabled} ? 1 : 0;
# }

#
# sub _job_clear_seen_reason {
#   my ( $job, $reason, $now ) = @_;
#   return unless ref( $job ) eq 'HASH' && length( $reason // '' );
#
#   # Keep history if you want it:
#   $job->{needs_user_resolved} ||= {};
#   $job->{needs_user_resolved}{$reason} = $now if defined $now;
#
#   delete $job->{needs_user_seen}{$reason}
#       if ref( $job->{needs_user_seen} ) eq 'HASH';
# }

# sub pump_tick {
#   my ( $class, $app, %args ) = @_;
#
#   state $inflight = 0;
#   if ( $inflight ) {
#     $app->log->debug(
#          prefix_dbg() . " pump_tick: skip (inflight)" );
#     return;
#   }
#   $inflight = 1;
#
#   my $ok  = eval { _pump_tick_impl( $class, $app, %args ); 1 };
#   my $err = $@;
#   $inflight = 0;
#
#   unless ( $ok ) {
#     chomp $err;
#     $app->log->error(
#               prefix_dbg() . " pump_tick died: $err " );
#   }
#
#   return;
# }
#
# sub _pump_tick_impl {
#   my ( $class, $app, %args ) = @_;
#   my $opts = $args{opts} || {};
#   my $jobs = _jobs_ref( $app );
#
#   my $sum = _jobs_summary( $jobs );
#   $app->log->debug(
#                    prefix_dbg() . " pump_tick: $sum " );
#   return unless %$jobs;
#
#   my $qbt;
#   my $qbt_ok = eval { $qbt = QBTL::QBT->new(); 1 };
#   unless ( $qbt_ok ) {
#     my $err = "$@ ";
#     chomp $err;
#     $app->log->error(
#              prefix_dbg() . " QBT init failed: $err " );
#     return;
#   }
#
#   my $now        = time;
#   my $tick_cache = {};     # per invocation cache
#   for my $ih ( sort keys %$jobs ) {
#     my $job = $jobs->{$ih};
#     next if ref( $job ) ne 'HASH';
#
# REINIT: {
#       last REINIT unless ( $job->{status} // '' ) eq 'needs_user';
#       my $reason = $job->{needs_user_reason} // $job->{interactive_reason}
#           // '';
#       last REINIT unless $reason eq 'renamed_root_class';
#       my $enabled = $app->defaults->{qbt}{prefs}{temp_path_enabled}; = _qbt_temp_path_enabled_cached( $qbt,
$tick_cache );
#       $app->log->debug(   basename( __FILE__ ) . ":"
#                         . __LINE__
#                         . "  maybe_reinit_needs_user ih: "
#                         . ( $job->{ih} // '' )
#                         . " temp_path_enabled: "
#                         . ( defined( $enabled ) ? $enabled : '(undef)' ) );
#       last REINIT if !defined( $enabled ) || $enabled;
#
#       # re-init
#       $job->{status}          = 'queued';
#       $job->{stage}           = 'queued';
#       $job->{step_idx}        = 0;
#       $job->{steps}           = _build_steps_add_one(  $job );
#       $job->{next_ts}         = $now + 0.5;
#       $job->{ts_updated}      = $now;
#       $job->{terminal_logged} = 0;
#
#       delete $job->{recheck_requested_ts};
#       delete $job->{recheck_last_check0};
#       delete $job->{stability_probe_inserted};
#     }
#     my $status   = $job->{status}   // '';
#     my $stage    = $job->{stage}    // '';
#     my $step_idx = $job->{step_idx} // 0;
#
#     my $is_terminal = ( $status =~ /^(done|failed|needs_user|needs_triage)$/ );
#     if ( $is_terminal ) {
#       delete $job->{next_ts};
#
#       next if $job->{terminal_logged};
#       $job->{terminal_logged} = 1;
#
#       $app->log->debug(
#          sprintf(
#            "Q->[JOB] ih: %s status: %s stage: %s step_idx: %s %s ( terminal ) ",
#            $ih, $status, $stage, $step_idx ) );
#       next;
#     }
#
#     # non-terminal jobs: next_ts defaults to " now " if missing/garbage
#     my $next_ts = $job->{next_ts};
#     $next_ts = $now if !defined( $next_ts ) || $next_ts !~ /^\d+$/;
#
#     my $wait = $next_ts - $now;
#
#     $app->log->debug(
#             basename( __FILE__ ) . ":"
#           . __LINE__
#           . sprintf(
# " Q->[JOB]  ih: %s status: %s stage: %s step_idx: %s next: %s now: %s wait: %+ ds ",
#         short_ih( $ih ), $status,                $stage,
#         $step_idx,       epoch2time( $next_ts ), epoch2time( $now ),
#         $wait ) );
#     next if $next_ts > $now;
#
#     my $steps = $job->{steps};
#     if ( ref( $steps ) ne 'ARRAY' ) {
#       $steps = [];
#       $job->{steps} = $steps;
#     }
#
#     # ---- TERMINAL: OUT OF STEPS -> DONE ----
#     if ( $step_idx >= @$steps ) {
#       _job_mark_done( $app, $job, $now );
#       next;
#     }
#
#     if ( @$steps ) {
#       my $remain = @$steps - $step_idx;
#       $remain = 0 if $remain < 0;
#
#       my $ops = join(
#         ", ",
#         map {
#           ( ref( $_ ) eq 'HASH' )
#               ? (
#             $_->{op} // "
#     ? " )
#               : "
#           ? "
#         } @$steps[ $step_idx .. $#$steps ] );
#
#       $app->log->debug(   basename( __FILE__ ) . ":"
#                         . __LINE__
#                         . " Q->[PLAN] ih: "
#                         . short_ih( $ih )
#                         . " steps_remaining: $remain ops: [$ops] " );
#     }
#
#     my $step = $steps->[$step_idx];
#     if ( ref( $step ) ne 'HASH' ) {
#       _job_mark_failed( $app, $job, $now,
#                         " bad step at idx: $step_idx ( not a HASH ) " );
#       next;
#     }
#
#     $step->{tries} = 0 if !defined $step->{tries};
#
#     my $op = $step->{op} // '';
#     $job->{stage} = $op;
#
#     my ( $result, $why, $hint ) =
#         _run_step( $app, $qbt, $job, $step, $tick_cache );
#
#     $app->log->debug(   basename( __FILE__ ) . ":"
#                       . __LINE__
#                       . "  step ih: $ih op: $op result: $result why: "
#                       . ( $why // '' ) );
#
#     if ( $result eq 'advance' ) {
#       _job_advance_step( $job, $now );
#       next;
#     }
#
#     if ( $result eq 'retry' ) {
#       my $rr = _job_retry_step( $app, $job, $step, $now, $why );
#
#       # If the retry handler decided this job now " needs_user ",
#       # remove torrent from qBt (keep data) right now.
#       if ( defined( $rr ) && $rr eq 'needs_user' ) {
#         _qbt_remove_torrent( $app, $qbt, $ih, delete_files => 0 );
#         next;
#       }
#
#       next;
#     }
#
#     if ( $result eq 'interactive' ) {
#       _qbt_remove_torrent( $app, $qbt, $ih, delete_files => 0 );    # keep data
#
#       my $reason = $why  || 'unknown';
#       my $detail = $hint || '';
#
#       _job_mark_needs_user( $app, $job, $now, $reason, $why, $detail );
#       next;
#     }
#
#     if ( $result eq 'renamed_root' ) {
#
#       # Otherwise: embrace failure → quarantine bucket
#       _qbt_remove_torrent( $app, $qbt, $ih, delete_files => 0 );    # keep data
#       _job_mark_renamed_root( $app, $job, $now, $why, $hint );
#       next;
#     }
#
# #######################################
#     # this is the edge-case catcher.
# #######################################
#
#     my $is_rename_symlink_class =
#         ( ref( $job->{rename} ) eq 'HASH' ) || ( $job->{symlink_used} );
#     if ( $is_rename_symlink_class ) {
#       _job_mark_needs_triage(
#                               $app,
#                               $job,
#                               $now,
#                               'unexpected_result',
#                               ( $why || " failed( result: $result ) " ),
#                               " op = "
#                                   . ( $job->{stage} // '' )
#                                   . " step_idx = "
#                                   . ( $job->{step_idx} // 0 ) );
#       next;
#     }
#
#     _job_mark_failed(
#       $app, $job, $now, $why
#           || " failed( result: $result )
#           " );
#   }

#   return;
# }
# sub _qbt_temp_path_enabled_cached {
#   my ( $qbt, $tick_cache ) = @_;
#   $tick_cache ||= {};
#   $tick_cache->{prefs} ||= {};
#
#   return $tick_cache->{prefs}{temp_path_enabled}
#       if exists $tick_cache->{prefs}{temp_path_enabled};
#
#   # wrapper should implement this; see note below
#   my $prefs = eval { $qbt->api_qbt_preferences() };
#
#   my $enabled = $app->defaults->{qbt}{prefs}{temp_path_enabled}; = undef;
#   if ( ref( $prefs ) eq 'HASH' && exists $prefs->{temp_path_enabled} ) {
#     $enabled = $prefs->{temp_path_enabled} ? 1 : 0;
#   }
#
#   $tick_cache->{prefs}{temp_path_enabled} = $enabled;    # can be undef
#   return $enabled;
# }
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

  # this will likely invoke re-check death in qbt
  push @steps, {op => 'remove_symlink', tries => 0};

  # embrace failure: let it die naturally then verify it died
  #push @steps, {op => 'probe_checking', tries => 0};

  # prove it won't re-fire
  push @steps, {op => 'recheck_again',        tries => 0};
  push @steps, {op => 'confirm_not_checking', tries => 0};

  # now we know we must go interactive
  push @steps, {op => 'interactive', tries => 0};

  # only NOW remove torrent from qBt (keep data)
  push @steps, {op => 'delete_torrent', tries => 0};

  return \@steps;
}
=cut
