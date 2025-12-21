package QBTL::LocalCache;
use common::sense;

use JSON qw(encode_json decode_json);
use JSON::PP ();
use File::Temp qw(tempfile);
use File::Slurp qw(read_file write_file);

use QBTL::Scan;
use QBTL::Parse;

sub cache_path {
  my (%args) = @_;
  my $root = $args{root_dir} || '.';
  return "$root/db/local_by_ih_cache.json";
}

sub load_local_by_ih {
  my (%args) = @_;
  my $path = cache_path(%args);

  return undef unless -e $path;

  my $json = read_file($path);
  my $data = decode_json($json);

  return (ref($data) eq 'HASH') ? $data : undef;
}

sub build_local_by_ih {
  my (%args) = @_;
  my $opts_local = $args{opts_local} || {};
  my $root_dir   = $args{root_dir}   || '.';

  my $scan   = QBTL::Scan::run(opts => $opts_local);
  my $parsed = QBTL::Parse::run(all_torrents => $scan->{torrents}, opts => $opts_local);

  my $local_by_ih = $parsed->{by_infohash} || $parsed->{infohash_map} || {};
  $local_by_ih = {} if ref($local_by_ih) ne 'HASH';

  my $path = cache_path(root_dir => $root_dir);

  my $json = JSON::PP->new
    ->utf8
    ->pretty
    ->canonical(1)
    ->encode($local_by_ih);

  # atomic write
  my ($fh, $tmp) = tempfile("$path.XXXX", UNLINK => 0);
  print {$fh} $json;
  close $fh;
  rename $tmp, $path or die "rename($tmp → $path): $!";

  return $local_by_ih;
}

sub get_local_by_ih {
  my (%args) = @_;
  my $loaded = load_local_by_ih(%args);
  return $loaded if $loaded;

  return build_local_by_ih(%args);
}

1;
