package QBTL::QBT;
use common::sense;
use HTTP::Cookies;
use LWP::UserAgent;

use Mojo::JSON qw(decode_json);

#use QBittorrent;   # legacy bootstrap only

sub new {
  Logger::debug("#	new");
  my ($class, $opts) = @_;
  my $self = {base_url => $opts->{base_url} || 'http://localhost:8080',
              username => $opts->{username} || 'admin',
              password => $opts->{password} || 'adminadmin',
              ua       => LWP::UserAgent->new(cookie_jar => HTTP::Cookies->new),
              %$opts,};
  bless $self, $class;
  $self->_login;
  return $self;
}

sub _login {
  Logger::debug("#	_login");
  my $self = shift;
  my $res = $self->{ua}->post("$self->{base_url}/api/v2/auth/login",
                              {username => $self->{username},
                               password => $self->{password}
                              });

  die "Login failed: " . $res->status_line unless $res->is_success;
}

# ------------------------------
# Read
# ------------------------------

sub get_torrents_info {
  my ($self, %args) = @_;

  my $url = "$self->{base_url}/api/v2/torrents/info";

  # Optional filter: hashes => '40hex' or '40hex|40hex|...'
  if (defined $args{hashes} && length $args{hashes})
  {
    my $h = $args{hashes};
    $h =~ s/\s+//g;

    # canonical: lower-hex only (fail loudly if anything else shows up)
    die "bad hashes" unless $h =~ /^[0-9a-f]{40}(?:\|[0-9a-f]{40})*$/;

    $url .= "?hashes=$h";
  }

  my $res = $self->{ua}->get($url);
  die "Failed to get torrents: " . $res->status_line unless $res->is_success;

  my $torrents = decode_json($res->decoded_content);
  return $torrents;    # arrayref of hashrefs
}

sub torrent_info_one {
  my ($self, $hash) = @_;
  die "bad hash" unless defined($hash) && $hash =~ /^[0-9a-f]{40}$/;

  my $arr = $self->get_torrents_info(hashes => $hash);
  return {} unless ref($arr) eq 'ARRAY' && @$arr && ref($arr->[0]) eq 'HASH';
  return $arr->[0];
}

sub torrent_exists {
  my ($self, $hash) = @_;
  my $t = $self->torrent_info_one($hash);
  return (ref($t) eq 'HASH' && ($t->{hash} // '') =~ /^[0-9a-f]{40}$/) ? 1 : 0;
}

sub get_torrents_infohash {
  my ($self) = @_;

  my $list = $self->get_torrents_info() || [];
  my %by_ih;

  for my $t (@$list)
  {
    next if ref($t) ne 'HASH';
    my $h = $t->{hash} // '';

    # canonical: lower-hex only
    next unless $h =~ /^[0-9a-f]{40}$/;

    $by_ih{$h} = $t;
  }

  return \%by_ih;    # { infohash => {...} }
}

# ------------------------------
# Actions
# ------------------------------

sub add_torrent_file {
  my ($self, $torrent_path, $savepath) = @_;

  require HTTP::Request::Common;

  my @content = (torrents => [$torrent_path]);

  # qBittorrent expects the field name "savepath"
  if (defined $savepath && length $savepath)
  {
    push @content, (savepath => $savepath);
  }

  my $req = HTTP::Request::Common::POST("$self->{base_url}/api/v2/torrents/add",
                                        Content_Type => 'form-data',
                                        Content      => \@content,);

  my $res = $self->{ua}->request($req);

  return {ok   => ($res->is_success ? 1 : 0),
          code => ($res->code            // 0),
          body => ($res->decoded_content // ''),};
}

sub recheck_hash {
  my ($self, $hash) = @_;
  die "bad hash" unless defined $hash && $hash =~ /^[0-9a-f]{40}$/;

  require HTTP::Request::Common;

  my $req = HTTP::Request::Common::POST(
                            "$self->{base_url}/api/v2/torrents/recheck",
                            Content_Type => 'application/x-www-form-urlencoded',
                            Content      => [hashes => $hash],);

  my $res = $self->{ua}->request($req);

  die "Failed to recheck: "
      . $res->status_line
      . " body="
      . ($res->decoded_content // '')
      unless $res->is_success;

  return 1;
}

sub resume_hash {
  my ($self, $hash) = @_;
  die "bad hash" unless defined($hash) && $hash =~ /^[0-9a-f]{40}$/;

  my $res =
      $self->{ua}
      ->post("$self->{base_url}/api/v2/torrents/resume", {hashes => $hash},);

  die "Failed to resume: "
      . $res->status_line
      . " body="
      . ($res->decoded_content // '')
      unless $res->is_success;

  return 1;
}

sub pause_hash {
  my ($self, $hash) = @_;
  die "bad hash" unless defined($hash) && $hash =~ /^[0-9a-f]{40}$/;

  my $res =
      $self->{ua}
      ->post("$self->{base_url}/api/v2/torrents/pause", {hashes => $hash},);

  die "Failed to pause: "
      . $res->status_line
      . " body="
      . ($res->decoded_content // '')
      unless $res->is_success;

  return 1;
}

1;
