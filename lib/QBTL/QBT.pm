package QBTL::QBT;
use common::sense;
use HTTP::Cookies;
use LWP::UserAgent;

use Mojo::JSON qw(decode_json);
#use QBittorrent;   # legacy bootstrap only

sub new {
	Logger::debug("#	new");
    my ($class, $opts) = @_;
    my $self = {
        base_url => $opts->{base_url} || 'http://localhost:8080',
        username => $opts->{username} || 'admin',
        password => $opts->{password} || 'adminadmin',
        ua       => LWP::UserAgent->new(cookie_jar => HTTP::Cookies->new),
        %$opts,
    };
    bless $self, $class;
    $self->_login;
    return $self;
}

#   my $self = bless {
#     opts     => $opts,
#     legacy   => $legacy,
#     ua       => $legacy->{ua},
#     base_url => $legacy->{base_url},
#   }, $class;
#
#   die "QBTL::QBT->new: missing ua/base_url (legacy init failed?)"
#     unless $self->{ua} && $self->{base_url};
#
#   return $self;
# }

sub _login {
	Logger::debug("#	_login");
    my $self = shift;
    my $res = $self->{ua}->post(
        "$self->{base_url}/api/v2/auth/login",
        {
            username => $self->{username},
            password => $self->{password}
        }
    );

    die "Login failed: " . $res->status_line unless $res->is_success;
}
#sub _legacy { shift->{legacy} }

# ------------------------------
# Read
# ------------------------------

sub get_torrents_info {
  my ($self) = @_;

  my $res = $self->{ua}->get("$self->{base_url}/api/v2/torrents/info");
  die "Failed to get torrents: " . $res->status_line unless $res->is_success;

  my $torrents = decode_json($res->decoded_content);
  return $torrents; # arrayref of hashrefs
}

sub get_torrents_infohash {
  my ($self) = @_;

  my $list = $self->get_torrents_info() || [];
  my %by_ih;

  for my $t (@$list) {
    next if ref($t) ne 'HASH';
    my $h = $t->{hash} // '';
    next unless $h =~ /^[0-9a-fA-F]{40}$/;
    $by_ih{ lc($h) } = $t;
  }

  return \%by_ih; # { infohash => {...} }
}

sub torrent_info {
  my ($self, $hash) = @_;
  die "bad hash" unless defined($hash) && $hash =~ /^[0-9a-f]{40}$/;

  my $res = $self->{ua}->get("$self->{base_url}/api/v2/torrents/info?hashes=$hash");
  die "torrent_info failed: " . $res->status_line unless $res->is_success;

  my $arr = decode_json($res->decoded_content);
  return (ref($arr) eq 'ARRAY' && @$arr) ? ($arr->[0] || {}) : {};
}

# Keep the old name so callers don't change.
sub torrent_info_one {
  my ($self, $hash) = @_;
  return $self->torrent_info($hash);
}

sub torrent_exists {
  my ($self, $hash) = @_;
  my $t = $self->torrent_info_one($hash);
  return (ref($t) eq 'HASH' && ($t->{hash} // '') =~ /^[0-9a-f]{40}$/) ? 1 : 0;
}

# ------------------------------
# Actions
# ------------------------------
sub add_torrent_file {
  my ($self, $torrent_path, $savepath) = @_;

  require HTTP::Request::Common;

  my @content = (torrents => [$torrent_path]);

  # THIS is the key: qBittorrent expects the field name "savepath"
  if (defined $savepath && length $savepath) {
    push @content, (savepath => $savepath);
  }

  my $req = HTTP::Request::Common::POST(
    "$self->{base_url}/api/v2/torrents/add",
    Content_Type => 'form-data',
    Content      => \@content,
  );

  my $res = $self->{ua}->request($req);

  return {
    ok   => ($res->is_success ? 1 : 0),
    code => ($res->code // 0),
    body => ($res->decoded_content // ''),
  };
}


sub recheck_hash {
  my ($self, $hash) = @_;
  die "bad hash" unless defined $hash && $hash =~ /^[0-9a-f]{40}$/i;

  require HTTP::Request::Common;

  my $req = HTTP::Request::Common::POST(
    "$self->{base_url}/api/v2/torrents/recheck",
    Content_Type => 'application/x-www-form-urlencoded',
    Content      => [ hashes => $hash ],
  );

  my $res = $self->{ua}->request($req);

  die "Failed to recheck: " . $res->status_line . " body=" . ($res->decoded_content // '')
    unless $res->is_success;

  return 1;
}

sub resume_hash {
  my ($self, $hash) = @_;
  die "bad hash" unless defined($hash) && $hash =~ /^[0-9a-f]{40}$/;

  my $res = $self->{ua}->post(
    "$self->{base_url}/api/v2/torrents/resume",
    form => { hashes => $hash },
  );

  die "Failed to resume: " . $res->status_line unless $res->is_success;
  return 1;
}

sub pause_hash {
  my ($self, $hash) = @_;
  die "bad hash" unless defined($hash) && $hash =~ /^[0-9a-f]{40}$/;

  my $res = $self->{ua}->post(
    "$self->{base_url}/api/v2/torrents/pause",
    form => { hashes => $hash },
  );

  die "Failed to pause: " . $res->status_line unless $res->is_success;
  return 1;
}

1;
