package QBTL::QBT;
use common::sense;
use Encode         qw(encode_utf8);
use File::Basename qw(
    basename
    dirname
);
use HTTP::Cookies;
use LWP::UserAgent;
use Mojo::JSON qw( decode_json );
use URI;
use URI::Escape qw(uri_escape uri_escape_utf8);

use QBTL::Logger;
use QBTL::Utils qw ( prefix_dbg );

use Exporter 'import';
our @EXPORT_OK = qw(qbt_echo);

# ------------------------------
# Constructor / Auth
# {base_url}/api/v2/auth/
# ------------------------------

sub new {
  my ( $class, $opts ) = @_;
  $opts ||= {};
  die "caller must not pass ua" if exists $opts->{ua};

  # Auto-enable HTTP dump in dev_mode unless caller explicitly set it
  if ( !exists $opts->{debug_http} ) {
    my $app = $opts->{app};

    my $dev = ( ref( $app ) && $app->defaults->{dev_mode} ) ? 1 : 0;
    $opts->{debug_http} = $dev;
  }

  my $self = {
              base_url     => $opts->{base_url}     || 'http://localhost:8080',
              username     => $opts->{username}     || 'admin',
              password     => $opts->{password}     || 'adminadmin',
              die_on_error => $opts->{die_on_error} || 0,
              ua           => LWP::UserAgent->new(
                                         cookie_jar => HTTP::Cookies->new,
                                         timeout => ( $opts->{timeout} // 3 ),
              ),
              %$opts,};

  # optional: keep-alive off while debugging weirdness
  # $self->{ua}->conn_cache(undef);

  # --- HTTP debug (request + response dumps) ---
  if ( $opts->{debug_http} ) {
    $self->{ua}->add_handler( request_send  => sub { $_[0]->dump; return } );
    $self->{ua}->add_handler( response_done => sub { $_[0]->dump; return } );
  }

  bless $self, $class;
}

sub api_qbt_login {

  #  QBTL::Logger::debug( "#\tapi_qbt_login" );
  my ( $self ) = shift;

  my $res = $self->{ua}->post(
                               "$self->{base_url}/api/v2/auth/login",
                               {
                                username => $self->{username},
                                password => $self->{password},
                               } );

  unless ( $res->is_success ) {
    return
        $self->_fail(
                      "auth/login failed: " . $res->status_line,
                      {
                       ok   => 0,
                       code => ( $res->code            // 0 ),
                       body => ( $res->decoded_content // '' ),
                      } );
  }

  return {ok => 1, code => ( $res->code // 200 ), body => ''};
}

sub qbt_echo {
  my ( $app, %opt ) = @_;
  my $want_api = $opt{want_api} ? 1 : 0;

  my $out = {
    pid      => 0,
    qbt_up   => 0,        # explicit proc status
    want_api => $want_api,# echoes whether we probed
    api_ok   => 0,
    api_ts   => 0,        # "API status known" only if we set this
    echo_ts  => time,
    echo_err => '',
  };

  my $pid = _find_qbt_pid();
  $out->{pid} = $pid;

  unless ( $pid ) {
    $out->{echo_err} = 'qbt not running';
    $app->log->debug(
      prefix_dbg()
        . $out->{echo_err}
        . " want_api: $want_api pid: " . ( $out->{pid} || 0 )
    );
    return $out;
  }

  $out->{qbt_up} = 1;

  # 🚦 HARD GATE — do NOT touch API unless explicitly asked
  return $out unless $want_api;

  # ---- API probe (tight timeout) ----
  $out->{api_ts} = time;   # mark "known" once we attempt probe

  my $ok = eval {
    local $SIG{ALRM} = sub { die "api timeout\n" };
    alarm 2;

    my $qbt = QBTL::QBT->new( { timeout => 2 } );
    $qbt->api_qbt_login();

    alarm 0;
    1;
  };

  if ( $ok ) {
    $out->{api_ok} = 1;
    $out->{echo_err} = '';
  }
  else {
    $out->{api_ok} = 0;
    $out->{echo_err} = $@ || 'api probe failed';
    $out->{echo_err} =~ s/\s+\z//;
  }

  $app->log->debug(
    prefix_dbg()
      . ( length($out->{echo_err}) ? $out->{echo_err} : 'api ok' )
      . " want_api: $want_api pid: " . ( $out->{pid} || 0 )
  );

  return $out;
}



# ------------------------------
#
# qBittorrent application level methods
# /api/v2/app/
# referred to as api_qbt_<function> to not confuse it with $app-> stuff later
#
# ------------------------------
sub api_app_preferences {
  my ( $self ) = @_;

  my $url = "$self->{base_url}/api/v2/app/preferences";

  for my $attempt ( 1 .. 2 ) {
    my $res = $self->{ua}->get( $url );

    if ( $res->is_success ) {
      my $prefs = eval { decode_json( $res->decoded_content ) };
      die "GET app/preferences JSON decode failed: $@" if $@;
      die "GET app/preferences did not return HASH"
          unless ref( $prefs ) eq 'HASH';
      return $prefs;
    }

    my $code = $res->code // 0;

    # session expired -> login -> retry once
    if ( ( $code == 401 || $code == 403 ) && $attempt == 1 ) {
      $self->api_qbt_login();    # dies if it can't fix it
      next;
    }

    die "GET app/preferences failed: " . $res->status_line;
  }
}

# ------------------------------
# API (Read)
# ------------------------------

sub api_torrents_delete {
  my ( $self, $ih, $deleteFiles ) = @_;
  die "missing hashes" unless defined && length;

  $self->_validate_hashes_pipe( $ih );

  # qBt accepts "true/false", be we're not taking the chance.
  $deleteFiles = 'false';

  return
      $self->_api_post_form(
                             '/api/v2/torrents/delete',
                             {
                              hashes      => $ih,
                              deleteFiles => $deleteFiles,
                             } );
}

sub api_torrents_info {
  my ( $self, %args ) = @_;

  # fail fast if qbt is clearly down
  my $eh = eval {
    QBTL::QBT::qbt_echo(
                         undef,
                         want_api => 0,
                         base_url => $self->{base_url} );
  } || {};

  #   warn "[qbt_echo] up=$eh->{qbt_up} pid=" . ( $eh->{pid} // 0 )
  #       . " host=" . ( $eh->{host} // '?' )
  #       . " port=" . ( $eh->{port}     // '?' )
  #       . " err="  . ( $eh->{echo_err} // '' ) . "\n";

  my $url = "$self->{base_url}/api/v2/torrents/info";

  if ( defined $args{hashes} && length $args{hashes} ) {
    my $h = $self->_validate_hashes_pipe( $args{hashes} );
    $url .= "?hashes=" . uri_escape( $h );
  }

  for my $try ( 1 .. 2 ) {
    my $res = $self->{ua}->get( $url );

    unless ( $res->is_success ) {
      my $code = $res->code // 0;
      if ( $try == 1 && ( $code == 401 || $code == 403 ) ) {
        $self->api_qbt_login();    # should die if it can't fix it
        next;
      }
      die "qbt GET /torrents/info failed: " . $res->status_line;
    }
    my $body = $res->decoded_content // '';
    my $list = eval { decode_json( $body ) };
    die "qbt GET /torrents/info JSON decode failed: $@"
        if $@;
    die "qbt GET /torrents/info expected ARRAY got "
        . ( ref( $list ) || 'SCALAR' )
        unless ref( $list ) eq 'ARRAY';
    return $list;
  }
  die "unreachable";
}

sub api_torrents_info_one {
  my ( $self, $ih ) = @_;
  unless ( ref( $self ) && ref( $self ) eq __PACKAGE__ ) {
    die "api_torrents_info_one called without object";
  }
  $self->_validate_hash( $ih );

  my $arr = $self->api_torrents_info( hashes => $ih );
  return {} unless ref $arr eq 'ARRAY' && @$arr && ref $arr->[0] eq 'HASH';

  return $arr->[0];
}

sub api_torrents_files {
  my ( $self, $ih ) = @_;
  $self->_validate_hash( $ih );

  my $url = "$self->{base_url}/api/v2/torrents/files?hash=$ih";

  for my $attempt ( 1 .. 2 ) {
    my $res = $self->{ua}->get( $url );

    unless ( $res->is_success ) {
      my $code = $res->code // 0;

      # auth/session died -> login and retry once
      if ( ( $code == 401 || $code == 403 ) && $attempt == 1 ) {
        my $ok = eval { $self->api_qbt_login(); 1 };
        next if $ok;
      }

      return
          $self->_fail(
                        "GET torrents/files failed: " . $res->status_line,
                        {
                         ok   => 0,
                         code => $code,
                         body => ( $res->decoded_content // '' ),
                        } );
    }

    my $json = eval { decode_json( $res->decoded_content ) };
    if ( $@ ) {
      return
          $self->_fail(
                        "GET torrents/files JSON decode failed: $@",
                        {
                         ok   => 0,
                         code => ( $res->code            // 0 ),
                         body => ( $res->decoded_content // '' ),
                        } );
    }

    if ( ref( $json ) ne 'ARRAY' ) {
      my $body = $res->decoded_content // '';
      $body =~ s/\s+/ /g;
      $body = substr( $body, 0, 160 );

      return
          $self->_fail(
                        "GET torrents/files: expected ARRAY got "
                            . ( ref( $json ) || 'SCALAR' ),
                        {ok => 0, code => ( $res->code // 0 ), body => $body} );
    }

    return $json;
  }
}

sub api_torrents_infohash_map {
  my ( $self ) = @_;

  my $list = $self->api_torrents_info();
  $list = [] unless ref( $list ) eq 'ARRAY';

  my %by_ih;

  for my $t ( @$list ) {
    next if ref $t ne 'HASH';

    my $ih = $t->{hash} // '';
    next unless $ih =~ /^[0-9a-f]{40}$/;

    $by_ih{$ih} = $t;
  }

  return \%by_ih;
}

# ------------------------------
# Convenience (non-API)
# ------------------------------

# sub _torrent_exists__via_api_torrents_info_one {
#   my ( $self, $hash ) = @_;
#   my $t = $self->api_torrents_info_one( $hash );
#
#   return ( ref $t eq 'HASH' && ( $t->{hash} // '' ) =~ /^[0-9a-f]{40}$/ )
#       ? 1
#       : 0;
# }

# ------------------------------
# API (Actions)
# ------------------------------

sub api_torrents_add {
  my ( $self, $torrent_path, $savepath ) = @_;
  return $self->_api_torrents_add__multipart( $torrent_path, $savepath );
}

sub api_torrents_recheck {
  my ( $self, $ih ) = @_;
  $self->_validate_hash( $ih );

  return $self->_api_post_form( '/api/v2/torrents/recheck', {hashes => $ih}, );
}

sub api_torrents_setLocation {
  my ( $self, $ih, $location ) = @_;
  $self->_validate_hash( $ih );
  die "missing location" unless defined $location && length $location;

  return
      $self->_api_post_form( '/api/v2/torrents/setLocation',
                             {hashes => $ih, location => $location}, );
}

sub api_torrents_setDownloadPath {
  my ( $self, $ih, $downloadPath ) = @_;
  $self->_validate_hash( $ih );
  die "missing downloadPath"
      unless defined $downloadPath && length $downloadPath;
  return
      $self->_api_post_form(
                             '/api/v2/torrents/setDownloadPath',
                             {
                              hashes       => $ih,
                              downloadPath => $downloadPath
                             }, );
}

sub api_torrents_pause {
  my ( $self, $ih ) = @_;
  $self->_validate_hash( $ih );

  return $self->_api_post_form( '/api/v2/torrents/pause', {hashes => $ih}, );
}

sub api_torrents_resume {
  my ( $self, $ih ) = @_;
  $self->_validate_hash( $ih );

  return $self->_api_post_form( '/api/v2/torrents/resume', {hashes => $ih}, );
}

sub api_torrents_renameFolder {
  my ( $self, $ih, $oldPath, $newPath ) = @_;
  $self->_validate_hash( $ih );

  die "missing oldPath" unless defined $oldPath && length $oldPath;
  die "missing newPath" unless defined $newPath && length $newPath;

  my $out = $self->_api_post_form(
                                   '/api/v2/torrents/renameFolder',
                                   {
                                    hash    => $ih,
                                    oldPath => $oldPath,
                                    newPath => $newPath
                                   }, );

  return $out unless $out->{ok};

  my $files = eval { $self->api_torrents_files( $ih ) };
  if ( $@ || ref $files ne 'ARRAY' ) {
    return
        $self->_fail(
                  "renameFolder verify failed: " . ( $@ ? $@ : "no file list" ),
                  $out, );
  }

  my $want_prefix = "$newPath/";
  my $saw_new     = 0;

  for my $f ( @$files ) {
    next unless ref $f eq 'HASH';
    my $p = $f->{name} // $f->{path} // '';
    next unless length $p;
    if ( index( $p, $want_prefix ) == 0 || $p eq $newPath ) {
      $saw_new = 1;
      last;
    }
  }

  unless ( $saw_new ) {
    return
        $self->_fail(
                  "renameFolder verify: did not see paths under '$want_prefix'",
                  $out, );
  }

  return $out;
}

sub api_torrents_renameFile {
  my ( $self, $ih, $oldPath, $newPath ) = @_;
  $self->_validate_hash( $ih );

  die "missing oldPath" unless defined $oldPath && length $oldPath;
  die "missing newPath" unless defined $newPath && length $newPath;

  return
      $self->_api_post_form(
                             '/api/v2/torrents/renameFile',
                             {
                              hash    => $ih,
                              oldPath => $oldPath,
                              newPath => $newPath
                             }, );
}

sub api_torrents_stop {
  my ( $self, $ih, $stopFiles ) = @_;
  die "missing hashes" unless defined( $ih ) && length( $ih );

  $self->_validate_hashes_pipe( $ih );

  # qBt accepts "true/false", be we're not taking the chance.
  $stopFiles = 'false';

  return
      $self->_api_post_form(
                             '/api/v2/torrents/stop',
                             {
                              hashes    => $ih,
                              stopFiles => $stopFiles,
                             } );
}

# ------------------------------
# HELPERS
# ------------------------------

sub _api_post_form {
  my ( $self, $path, $form_href ) = @_;
  die "missing path" unless defined $path && length $path;

  $form_href ||= {};
  die "form must be HASH" if ref( $form_href ) && ref( $form_href ) ne 'HASH';

  my $url = "$self->{base_url}$path";

  for my $attempt ( 1 .. 2 ) {

    # Force deterministic x-www-form-urlencoded encoding
    my $u = URI->new( $url );
    $u->query_form( %$form_href );

    my $req = HTTP::Request->new( POST => $u->as_string );
    $req->header( 'Content-Type' => 'application/x-www-form-urlencoded' );

    # move params from URL into body (what qBt expects)
    my $body = $u->query // '';
    $u->query( undef );
    $req->uri( $u );
    $req->content( $body );

    if ( $self->{debug_http} ) {
      warn "[QBTL::QBT] POST $path\n";
      warn "[QBTL::QBT] url=" . $u->as_string . "\n";
      warn "[QBTL::QBT] body=$body\n";
    }

    my $res = $self->{ua}->request( $req );

    # success
    if ( $res->is_success ) {
      return {
              ok   => 1,
              code => ( $res->code            // 200 ),
              body => ( $res->decoded_content // '' ),};
    }

    my $code = $res->code // 0;

    # auth/session died -> login then retry once
    if ( ( $code == 401 || $code == 403 ) && $attempt == 1 ) {
      my $login = eval { $self->api_qbt_login() };
      next if !$@ && ref( $login ) eq 'HASH' && $login->{ok};

      # login failed -> fall through to failure below
    }

    return
        $self->_fail(
                      "POST failed: $path: " . $res->status_line,
                      {
                       ok   => 0,
                       code => ( $res->code            // 0 ),
                       body => ( $res->decoded_content // '' ),
                      } );
  }
}

sub _api_torrents_add__multipart {
  my ( $self, $torrent_path, $savepath ) = @_;

  unless ( defined $torrent_path && length $torrent_path ) {
    return $self->_fail( "missing torrent_path" );
  }
  unless ( -f $torrent_path ) {
    return $self->_fail( "torrent not found: $torrent_path" );
  }
  my @content = ( torrents => [$torrent_path] );

  if ( defined $savepath && length $savepath ) {
    push @content, ( savepath => $savepath );
  }

  my $res = $self->{ua}->post(
                               "$self->{base_url}/api/v2/torrents/add",
                               Content_Type => 'form-data',
                               Content      => \@content, );

  return {
          ok   => ( $res->is_success ? 1 : 0 ),
          code => ( $res->code            // 0 ),
          body => ( $res->decoded_content // '' ),};
}

sub _fail {
  my ( $self, $msg, $out ) = @_;
  $out ||= {ok => 0};

  $out->{ok}  = 0    unless defined $out->{ok};
  $out->{err} = $msg unless defined $out->{err} && length $out->{err};

  die $msg if $self->{die_on_error};
  return $out;
}

sub _find_qbt_pid {

  # macOS: pgrep is the simplest + fast
  my $pid = '';
  eval {
    $pid = qx{/usr/bin/pgrep -x qbittorrent 2>/dev/null};
    1;
  };
  $pid ||= '';
  $pid =~ s/\s+.*\z//s;    # first pid only
  $pid =~ s/\D//g;         # keep digits
  return $pid ? int( $pid ) : 0;
}

sub _qbt_health_stale {
  my ( $h, $max_age ) = @_;
  $max_age ||= 60;         # seconds
  return 1 unless $h && $h->{echo_ts};
  return ( time - $h->{echo_ts} ) > $max_age;
}

sub _validate_hash {
  my ( undef, $ih ) = @_;
  die "bad hash" unless defined $ih && $ih =~ /^[0-9a-f]{40}$/;
  return 1;
}

sub _validate_hashes_pipe {
  my ( undef, $ih ) = @_;
  die "bad hashes" unless defined $ih && length $ih;

  my $h = $ih;
  $h =~ s/\s+//g;

  die "bad hashes" unless $h =~ /^[0-9a-f]{40}(?:\|[0-9a-f]{40})*$/;
  return $h;
}

1;

=pod


=cut
