package QBTL::Plan;
use common::sense;

sub norm_ih {
  my ($ih) = @_;
  return '' if !defined $ih;
  $ih =~ s/^\s+|\s+$//g;
  $ih = lc($ih);
  return $ih;
}

sub normalize_hash_keys {
  my ($href) = @_;
  my %out;
  return \%out if ref($href) ne 'HASH';
  for my $k (keys %$href) {
    my $nk = norm_ih($k);
    next if $nk eq '';
    $out{$nk} = $href->{$k};
  }
  return \%out;
}

sub missing_infohashes {
  my (%args) = @_;
  my $local_by_ih = normalize_hash_keys($args{local_by_ih});
  my $qbt_set     = normalize_hash_keys($args{qbt_set});

  my @missing;
  for my $ih (keys %$local_by_ih) {
    next if exists $qbt_set->{$ih};
    push @missing, $ih;
  }

  my $overlap = 0;
  for my $ih (keys %$local_by_ih) {
    $overlap++ if exists $qbt_set->{$ih};
  }

  return {
    local_count => scalar(keys %$local_by_ih),
    qbt_count   => scalar(keys %$qbt_set),
    overlap     => $overlap,
    missing     => \@missing,
  };
}

1;
