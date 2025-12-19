package QBTL::Scan;
use common::sense;

sub run {
  my (%args) = @_;

  return {
    status => 'ok',
    message => 'scan stub (no real work yet)',
    found => 0,
  };
}

1;
