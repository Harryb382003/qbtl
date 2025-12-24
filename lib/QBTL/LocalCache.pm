package QBTL::LocalCache;
use common::sense;

use JSON::PP    ();
use File::Temp  qw(tempfile);
use File::Slurp qw(read_file write_file);

use QBTL::Scan;
use QBTL::Parse;

our %_MEMO;

sub cache_path {
  my (%args) = @_;
  my $root = $args{root_dir} || '.';
  return "$root/db/local_by_ih_cache.json";
}

sub load_local_by_ih {
  my (%args) = @_;
  my $path = cache_path(%args);

  return undef unless -e $path;

  my $json = read_file($path, binmode => ':raw');
  my $data = JSON::PP->new->utf8->decode($json);

  return (ref($data) eq 'HASH') ? $data : undef;
}

sub build_local_by_ih {
  my (%args)     = @_;
  my $opts_local = $args{opts_local} || {};
  my $root_dir   = $args{root_dir}   || '.';

  my $scan = QBTL::Scan::run(opts => $opts_local);
  my $parsed = QBTL::Parse::run(all_torrents => $scan->{torrents},
                                opts         => $opts_local);

  my $local_by_ih = $parsed->{by_infohash} || $parsed->{infohash_map} || {};
  $local_by_ih = {} if ref($local_by_ih) ne 'HASH';

  my $path = cache_path(root_dir => $root_dir);

  my $json = JSON::PP->new->utf8->pretty->canonical(1)->encode($local_by_ih);

  # atomic write
  my ($fh, $tmp) = tempfile("$path.XXXX", UNLINK => 0);
  binmode($fh, ':raw');
  print {$fh} $json;
  close $fh;
  rename $tmp, $path or die "rename($tmp → $path): $!";
  $_MEMO{$path} = {mtime => (stat($path))[9] || time(), data => $local_by_ih};
  return $local_by_ih;
}

sub get_local_by_ih {
  my (%args) = @_;
  my $path = cache_path(%args);

  # If file exists, use memoized decoded JSON unless mtime changed
  if (-e $path)
  {
    my $mtime = (stat($path))[9] || 0;

    my $m = $_MEMO{$path};
    if ($m && ($m->{mtime} // 0) == $mtime && ref($m->{data}) eq 'HASH')
    {
      return $m->{data};
    }

    # reload + decode once
    my $json = read_file($path);
    my $data = JSON::PP::decode_json($json);
    $data = {} if ref($data) ne 'HASH';

    $_MEMO{$path} = {mtime => $mtime, data => $data};
    return $data;
  }

  # No file yet -> build, then memoize
  my $data = build_local_by_ih(%args);
  $data = {} if ref($data) ne 'HASH';

  my $mtime = (-e $path) ? ((stat($path))[9] || 0) : time();
  $_MEMO{$path} = {mtime => $mtime, data => $data};

  return $data;
}

1;
