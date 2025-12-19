package QBTL::Devel;

use common::sense;


#####################################################################
#           						DEBUGGING ROUTES
#####################################################################

sub register_routes {
  my ($app) = @_;
  my $r = $app->routes;


  $r->get('/debug_ping' => sub {
    my $c = shift;
    $c->render(text => "debug ok");
  });


	$r->get('/ih_debug' => sub {
  my $c = shift;

  my $opts = { torrent_dir => "/" };

  my $scan = eval { QBTL::Scan::run(opts => $opts) };
  if ($@) { return $c->render(json => { error => "scan: $@" }); }

  my $qbt_set = eval { QBTL::QBT::infohash_set(opts => $opts) };
  if ($@) { return $c->render(json => { error => "qbt: $@" }); }

  my $parsed = eval { QBTL::Parse::run(all_torrents => $scan->{torrents}, opts => $opts, qbt_loaded_tor => $qbt_set) };
  if ($@) { return $c->render(json => { error => "parse: $@" }); }

  my $local_by_ih =
      $parsed->{by_infohash}
   || $parsed->{infohash_map}
   || {};

  my @local_keys = ref($local_by_ih) eq 'HASH' ? keys %$local_by_ih : ();
  my @qbt_keys   = ref($qbt_set)     eq 'HASH' ? keys %$qbt_set     : ();

  my $is_40hex = sub {
    my $s = shift // '';
    $s =~ s/^\s+|\s+$//g;
    return ($s =~ /^[0-9a-fA-F]{40}$/) ? 1 : 0;
  };

  my $count_40hex = sub {
    my @k = @_;
    my $n = 0;
    for (@k) { $n++ if $is_40hex->($_) }
    return $n;
  };

  my @local_sample = (sort @local_keys)[0 .. (@local_keys < 5 ? $#local_keys : 4)];
  my @qbt_sample   = (sort @qbt_keys)[0   .. (@qbt_keys   < 5 ? $#qbt_keys   : 4)];

  $c->render(json => {
    local_key_count     => scalar(@local_keys),
    qbt_key_count       => scalar(@qbt_keys),
    local_40hex_count   => $count_40hex->(@local_keys),
    qbt_40hex_count     => $count_40hex->(@qbt_keys),
    local_sample_keys   => \@local_sample,
    qbt_sample_keys     => \@qbt_sample,
  });
});

$r->get('/overlap_debug' => sub {
  my $c = shift;

  my $opts = { torrent_dir => "/" };  # temporary

  my $scan = eval { QBTL::Scan::run(opts => $opts) };
  if ($@) { return $c->render(json => { error => "scan: $@" }); }

  my $qbt_set = eval { QBTL::QBT::infohash_set(opts => $opts) };
  if ($@) { return $c->render(json => { error => "qbt: $@" }); }

  my $parsed = eval { QBTL::Parse::run(all_torrents => $scan->{torrents}, opts => $opts, qbt_loaded_tor => $qbt_set) };
  if ($@) { return $c->render(json => { error => "parse: $@" }); }

  my $local_by_ih = $parsed->{by_infohash} || $parsed->{infohash_map} || {};
  my @hits;

  if (ref($local_by_ih) eq 'HASH' && ref($qbt_set) eq 'HASH') {
    for my $ih (keys %$local_by_ih) {
      if (exists $qbt_set->{$ih}) {
        push @hits, $ih;
        last if @hits >= 10;
      }
    }
  }

  $c->render(json => {
    local_count   => (ref($local_by_ih) eq 'HASH' ? scalar(keys %$local_by_ih) : 0),
    qbt_count     => (ref($qbt_set) eq 'HASH' ? scalar(keys %$qbt_set) : 0),
    overlap_found => scalar(@hits),
    overlap_sample => \@hits,
  });
});


$r->get('/sample_local' => sub {
  my $c = shift;

  my $opts = { torrent_dir => "/" };  # temporary

  my $scan = eval { QBTL::Scan::run(opts => $opts) };
  if ($@) { return $c->render(json => { error => "scan: $@" }); }

  my $parsed = eval { QBTL::Parse::run(all_torrents => $scan->{torrents}, opts => $opts) };
  if ($@) { return $c->render(json => { error => "parse: $@" }); }

  my $local_by_ih = $parsed->{by_infohash} || $parsed->{infohash_map} || {};
  if (ref($local_by_ih) ne 'HASH' || !keys %$local_by_ih) {
    return $c->render(json => { error => "no local infohash map found" });
  }

  my @ih = sort keys %$local_by_ih;
  my @pick;
  for my $i (0..19) {
    last if $i > $#ih;
    my $h = $ih[$i];
    my $v = $local_by_ih->{$h};

    # value might be a path string OR a richer structure; handle both
#     my $path =
#         !ref($v) ? $v
#       : ref($v) eq 'HASH' ? ($v->{path} || $v->{torrent_path} || $v->{file} || '')
#       : '';
    my $path = ref($v) eq 'HASH' ? $v->{source_path} : '';

    push @pick, { infohash => $h, path => $path };
  }

  $c->render(json => { sample => \@pick });
});

use Data::Dumper;

$r->get('/local_entry' => sub {
  my $c  = shift;
  my $ih = $c->param('ih') // '';

  my $opts = { torrent_dir => "/" };

  my $scan = QBTL::Scan::run(opts => $opts);
  my $parsed = QBTL::Parse::run(
    all_torrents => $scan->{torrents},
    opts         => $opts,
  );

  my $local_by_ih = $parsed->{by_infohash}
                 || $parsed->{infohash_map}
                 || {};

  my $v = $local_by_ih->{$ih};

  $c->render(json => {
    infohash => $ih,
    ref_type => (defined $v ? ref($v) : 'undef'),
    dumped   => Dumper($v),
  });
});

$r->get('/qbt_name_is_hash' => sub {
  my $c = shift;

  my $opts = {};  # later: load config.json like legacy did

  my $qb = eval { QBittorrent->new($opts) };
  if ($@) { return $c->render(json => { error => "QBittorrent->new: $@" }); }

  # Try to get full torrent list from the legacy module.
#   my $list = eval {
#     if ($qb->can('get_torrents')) {
#       return $qb->get_torrents();                 # guessed
#     } elsif ($qb->can('torrents_info')) {
#       return $qb->torrents_info();                # guessed
#     } elsif ($qb->can('get_torrents_list')) {
#       return $qb->get_torrents_list();            # guessed
#     } else {
#       die "No method found to fetch torrent list (tried get_torrents/torrents_info/get_torrents_list)";
#     }
#   };
#   if ($@) { return $c->render(json => { error => "fetch list: $@" }); }

  my $list = eval { $qb->get_torrents_info() };
  if ($@) {
    return $c->render(json => { error => "fetch list: $@" });
  }

  $list = [] if ref($list) ne 'ARRAY';

  my @hits;
  for my $t (@$list) {
    next if ref($t) ne 'HASH';
    my $name = $t->{name} // '';
    my $hash = $t->{hash} // $t->{infohash} // '';
    my $progress = $t->{progress} // -1;
    next unless $name =~ /^[0-9a-fA-F]{40}$/;     # name *is* 40-hex
    push @hits, {
      name => $name,
      hash => $hash,
      save_path => ($t->{save_path} // ''),
      state => ($t->{state} // ''),
      progress  => $progress,
    };
    last if @hits >= 50;
  }

  my $zero = 0;
  $zero++ for grep { ($_->{progress} // -1) == 0 } @hits;
    $c->render(json => {
      found => scalar(@hits),
      found_progress_zero => $zero,
      sample => \@hits,
    });
  });
  return;


}

1;
