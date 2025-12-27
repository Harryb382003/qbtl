package QBTL::Queue;

use common::sense;

sub _jobs_ref {
  my ($app) = @_;
  $app->defaults->{queue_jobs} ||= {};    # { ih => { ...job... } }
  return $app->defaults->{queue_jobs};
}

sub enqueue_add_one {
  my ($class, $app, $ih, %args) = @_;
  die "bad ih" unless defined($ih) && $ih =~ /^[0-9a-f]{40}$/;

  my $jobs = _jobs_ref($app);

  my $job = (
             $jobs->{$ih} ||= {ih         => $ih,
                               steps      => [],
                               step_idx   => 0,
                               stage      => 'queued',
                               attempts   => 0,
                               next_ts    => time,
                               ts_created => time,
                               ts_updated => time,});

  # merge/patch fields
  $job->{ts_updated}  = time;
  $job->{source_path} = $args{source_path} if defined $args{source_path};
  $job->{savepath}    = $args{savepath}    if defined $args{savepath};
  $job->{rename}      = $args{pending_root_rename_data}
      if ref($args{pending_root_rename_data}) eq 'HASH';

  # build steps once
  if (!@{$job->{steps} || []})
  {
    $job->{steps}    = _build_steps_add_one($job);
    $job->{step_idx} = 0;
  }

  return $job;
}

sub _build_steps_add_one {
  my ($job) = @_;
  my @steps;

  push @steps,
      {op => 'wait_visible', tries => 0};    # confirms torrent exists in qbt
  if (ref($job->{rename}) eq 'HASH')
  {
    push @steps, {op => 'rename_root',    tries => 0};
    push @steps, {op => 'confirm_rename', tries => 0};
  }
  push @steps, {op => 'recheck',          tries => 0};
  push @steps, {op => 'confirm_checking', tries => 0};

  return \@steps;
}

# called by your Mojo::IOLoop recurring tick
sub pump_tick {
  my ($class, $app, %args) = @_;
  my $opts = $args{opts} || {};

  my $jobs = _jobs_ref($app);
  return unless %$jobs;

  my $qbt;
  eval { $qbt = QBTL::QBT->new(opts => $opts); 1 } or return;

  my $now = time;

  for my $ih (sort keys %$jobs)
  {
    my $job = $jobs->{$ih};
    next unless ref($job) eq 'HASH';
    next if ($job->{next_ts} || 0) > $now;
    next if ($job->{stage}   || '') =~ /^(done|failed)$/;

    my $steps = $job->{steps}    || [];
    my $i     = $job->{step_idx} || 0;

    if ($i >= @$steps)
    {
      $job->{stage} = 'done';
      next;
    }

    my $step = $steps->[$i];
    my ($result, $why) = _run_step($app, $qbt, $job, $step);

    if ($result eq 'advance')
    {
      $job->{step_idx}++;
      $job->{attempts}   = 0;
      $job->{next_ts}    = $now;          # run next step immediately next tick
      $job->{stage}      = $step->{op};
      $job->{last_error} = '';
      next;
    }

    if ($result eq 'retry')
    {
      my $a     = ++$job->{attempts};
      my $delay = 0.1 * (2**($a - 1));
      $delay             = 1.0 if $delay > 1.0;
      $job->{next_ts}    = $now + $delay;
      $job->{last_error} = $why || 'retry';
      next;
    }

    # fail hard
    $job->{stage}      = 'failed';
    $job->{last_error} = $why || 'failed';
    $job->{next_ts}    = $now + 9999999;
  }

  return;
}

sub _run_step {
  my ($app, $qbt, $job, $step) = @_;
  my $op = $step->{op} || '';

  if ($op eq 'wait_visible')
  {
    my $ok = eval { $qbt->torrent_exists($job->{ih}) };
    return ('retry', "torrent_exists died: $@") if $@;
    return $ok ? ('advance', '') : ('retry', "not visible yet");
  }

  if ($op eq 'rename_root')
  {
    my $rr = $job->{rename};
    return ('advance', '') unless ref($rr) eq 'HASH';

    my $res = eval {
      $qbt->rename_folder($job->{ih},
                          $rr->{torrent_top_lvl},
                          $rr->{drivespace_top_lvl});
    };
    return ('retry', "rename_folder died: $@") if $@;
    return ($res && $res->{ok})
        ? ('advance', '')
        : ('retry', "rename_folder not ok");
  }

  if ($op eq 'confirm_rename')
  {
    # best-effort confirm: use torrent files list / properties if you implement it
    # for now: advance (you can harden later)
    return ('advance', '');
  }

  if ($op eq 'recheck')
  {
    my $ok = eval { $qbt->recheck_hash($job->{ih}); 1 };
    return $ok ? ('advance', '') : ('retry', "recheck died: $@");
  }

  if ($op eq 'confirm_checking')
  {
    my $arr = eval { $qbt->get_torrents_info(hashes => $job->{ih}) };
    return ('retry', "get_torrents_info died: $@") if $@;
    return ('retry', "not visible yet") unless ref($arr) eq 'ARRAY' && @$arr;
    my $t0    = $arr->[0] || {};
    my $state = (ref($t0) eq 'HASH') ? ($t0->{state} // '') : '';
    return ($state =~ /^checking/i)
        ? ('advance', '')
        : ('retry', "state=$state");
  }

  return ('fail', "unknown op=$op");
}

1;
