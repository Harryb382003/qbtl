package QBTL::QBT;
use common::sense;
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
    my $res;
    my $ok = eval {

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

      $res = $self->{ua}->request( $req );
      1;
    };

    unless ( $ok ) {
      my $e = "$@";
      chomp $e;

      $self->_triage_http(
                           why       => 'http_die',
                           path      => $path,
                           form_href => $form_href,
                           attempt   => $attempt,
                           err       => $e, );

      return
          $self->_fail( "POST died: $path: $e",
                        {ok => 0, code => 0, body => ''} );
    }

=pod

#     unless ( $ok ) {
#       my $e = "$@";
#       chomp $e;
#
#       # TRIAGE (best-effort, never die from triage itself)
#       if ( my $app = $self->{app} ) {
#         eval {
#           my $ih = '';
#
#         # best-effort: some endpoints use 'hashes' (pipe-separated), some 'hash'
#           if ( exists $form_href->{hash} ) {
#             $ih = $form_href->{hash} // '';
#           }
#           elsif ( exists $form_href->{hashes} ) {
#             $ih = $form_href->{hashes} // '';
#           }
#
#           QBTL::Classify::classify_triage(
#                                            $app,
#                                            {
#                                             ih   => $ih,
#                                             key  => 'qbt_post_form',
#                                             path => $path,
#                                             err  => $e,
#                                            },
#                                            'http_message_bytes', );
#           1;
#         };
#       }
#
#       return
#           $self->_fail(
#                         "POST failed: $path: "
#                             . ( $res ? $res->status_line : '(no response)' ),
#                         {
#                          path => $path,
#                          ih   => (
#                                 $form_href->{hash} // $form_href->{hashes} // ''
#                          ),
#                          ok   => 0,
#                          code => ( $res ? ( $res->code // 0 ) : 0 ),
#                          body => (
#                                    $res ? ( $res->decoded_content // '' ) : ''
#                          ),
#                         } );
#     }

=cut

    # success
    unless ( $res || $res->is_success ) {
      $self->_triage_http(
                           why       => 'http_fail',
                           path      => $path,
                           form_href => $form_href,
                           attempt   => $attempt,
                           res       => $res, );
    }

    #     if ( $res && $res->is_success ) {
    #       return {
    #               ok   => 1,
    #               code => ( $res->code            // 200 ),
    #               body => ( $res->decoded_content // '' ),};
    #     }

    my $code = $res ? ( $res->code // 0 ) : 0;

    # auth/session died -> login then retry once
    if ( ( $code == 401 || $code == 403 ) && $attempt == 1 ) {
      my $login = eval { $self->api_qbt_login() };
      next if !$@ && ref( $login ) eq 'HASH' && $login->{ok};
    }

    return
        $self->_fail(
                      "POST failed: $path: "
                          . ( $res ? $res->status_line : '(no response)' ),
                      {
                       ok   => 0,
                       code => ( $res ? ( $res->code // 0 ) : 0 ),
                       body => (
                                 $res ? ( $res->decoded_content // '' ) : ''
                       ),
                      } );
  }
}

sub _fail {
  my ( $self, $msg, $extra ) = @_;
  $extra ||= {};

  # TRIAGE (best-effort; never die from triage)
  if ( my $app = $self->{app} ) {
    eval {
      QBTL::Classify::classify_triage(
                                       $app,
                                       {
                                        ih   => ( $extra->{ih} // '' ),
                                        key  => 'qbt_fail',
                                        path => ( $extra->{path} // '' ),
                                        err  => $msg,
                                        code => ( $extra->{code} // 0 ),
                                        body => ( $extra->{body} // '' ),
                                       },
                                       'qbt_fail', );
      1;
    };
  }

  return {
          ok   => 0,
          code => ( $extra->{code} // 0 ),
          body => ( $extra->{body} // '' ),
          err  => $msg,};
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

sub _triage_http {
  my ( $self, %arg ) = @_;

  my $app = $self->{app};
  return 0 unless $app;

  my $path      = $arg{path}      // '';
  my $form_href = $arg{form_href} // {};
  my $why       = $arg{why}       // 'http_fail';
  my $attempt   = $arg{attempt}   // 0;

  my $res = $arg{res};    # may be undef
  my $err = $arg{err};    # may be undef

  my $ih = '';
  if ( ref( $form_href ) eq 'HASH' ) {
    if ( exists $form_href->{hash} ) {
      $ih = $form_href->{hash} // '';
    }
    elsif ( exists $form_href->{hashes} ) {
      $ih = $form_href->{hashes} // '';
    }
  }

  my $code = ( $res ? ( $res->code // 0 )             : 0 );
  my $stl  = ( $res ? $res->status_line               : '' );
  my $body = ( $res ? ( $res->decoded_content // '' ) : '' );

  # don’t die if classify_triage hiccups
  eval {
    QBTL::Classify::classify_triage(
                                     $app,
                                     {
                                      ih          => $ih,
                                      key         => 'qbt_post_form',
                                      path        => $path,
                                      attempt     => $attempt,
                                      http_code   => $code,
                                      status_line => $stl,
                                      err  => ( defined $err ? $err : '' ),
                                      body => $body,
                                     },
                                     $why, );
    1;
  };

  return 1;
}

sub _validate_hash {
  my ( undef, $hash ) = @_;
  die "bad hash" unless defined $hash && $hash =~ /^[0-9a-f]{40}$/;
  return 1;
}

sub _validate_hashes_pipe {
  my ( undef, $hashes ) = @_;
  die "bad hashes" unless defined $hashes && length $hashes;

  my $h = $hashes;
  $h =~ s/\s+//g;

  die "bad hashes" unless $h =~ /^[0-9a-f]{40}(?:\|[0-9a-f]{40})*$/;
  return $h;
}

# ------------------------------
# Constructor / Auth
# {base_url}/api/v2/auth/
# ------------------------------

sub new {
  my ( $class, $opts ) = @_;
  $opts ||= {};

  my $self = {%$opts};

  $self->{base_url}     ||= 'http://localhost:8080';
  $self->{username}     ||= 'admin';
  $self->{password}     ||= 'adminadmin';
  $self->{die_on_error} ||= 0;

  $self->{ua} ||= LWP::UserAgent->new( cookie_jar => HTTP::Cookies->new );
  warn "[QBTL::QBT] new app_id="
      . ( defined( $self->{app} ) ? ( 0 + $self->{app} ) : 'undef' ) . "\n";
  bless $self, $class;
  return $self;
}

sub api_qbt_login {
  QBTL::Logger::debug( "#\tapi_qbt_login" );
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

# ------------------------------
# qBittorrent appliaction level methods
# /api/v2/app/
# referred to as api_qbt_<function> to not confuse it with $app-> stuff leter

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
  my ( $self, $hashes, $deleteFiles ) = @_;
  die "missing hashes" unless defined $hashes && length $hashes;

  # qBt accepts "true/false", be we're not taking the chance.
  $deleteFiles = 'false';

  return
      $self->_api_post_form(
                             '/api/v2/torrents/delete',
                             {
                              hashes      => $hashes,
                              deleteFiles => $deleteFiles,
                             } );
}

sub api_torrents_info {
  my ( $self, %args ) = @_;

  my $url = "$self->{base_url}/api/v2/torrents/info";

  if ( defined $args{hashes} && length $args{hashes} ) {
    my $h = $self->_validate_hashes_pipe( $args{hashes} );
    $url .= "?hashes=" . uri_escape( $h );
  }

  for my $try ( 1 .. 2 ) {
    my $res = $self->{ua}->get( $url );

    if ( !$res->is_success ) {
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
  my ( $self, $hash ) = @_;
  unless ( ref( $self ) && ref( $self ) eq __PACKAGE__ ) {
    die "api_torrents_info_one called without object";
  }
  $self->_validate_hash( $hash );

  my $arr = $self->api_torrents_info( hashes => $hash );
  return {} unless ref $arr eq 'ARRAY' && @$arr && ref $arr->[0] eq 'HASH';

  return $arr->[0];
}

sub api_torrents_files {
  my ( $self, $hash ) = @_;
  $self->_validate_hash( $hash );

  my $url = "$self->{base_url}/api/v2/torrents/files?hash=$hash";

  for my $attempt ( 1 .. 2 ) {
    my $res = $self->{ua}->get( $url );

    if ( !$res->is_success ) {
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

    my $h = $t->{hash} // '';
    next unless $h =~ /^[0-9a-f]{40}$/;

    $by_ih{$h} = $t;
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
  my ( $self, $hash ) = @_;
  $self->_validate_hash( $hash );

  return $self->_api_post_form( '/api/v2/torrents/recheck',
                                {hashes => $hash}, );
}

sub api_torrents_setLocation {
  my ( $self, $hash, $location ) = @_;
  $self->_validate_hash( $hash );
  die "missing location" unless defined $location && length $location;

  return
      $self->_api_post_form( '/api/v2/torrents/setLocation',
                             {hashes => $hash, location => $location}, );
}

sub api_torrents_setDownloadPath {
  my ( $self, $hash, $downloadPath ) = @_;
  $self->_validate_hash( $hash );
  die "missing downloadPath"
      unless defined $downloadPath && length $downloadPath;
  return
      $self->_api_post_form(
                             '/api/v2/torrents/setDownloadPath',
                             {
                              hashes       => $hash,
                              downloadPath => $downloadPath
                             }, );
}

sub api_torrents_pause {
  my ( $self, $hash ) = @_;
  $self->_validate_hash( $hash );

  return $self->_api_post_form( '/api/v2/torrents/pause', {hashes => $hash}, );
}

sub api_torrents_resume {
  my ( $self, $hash ) = @_;
  $self->_validate_hash( $hash );

  return $self->_api_post_form( '/api/v2/torrents/resume', {hashes => $hash}, );
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
  my ( $self, $hash, $oldPath, $newPath ) = @_;
  $self->_validate_hash( $hash );

  die "missing oldPath" unless defined $oldPath && length $oldPath;
  die "missing newPath" unless defined $newPath && length $newPath;

  return
      $self->_api_post_form(
                             '/api/v2/torrents/renameFile',
                             {
                              hash    => $hash,
                              oldPath => $oldPath,
                              newPath => $newPath
                             }, );
}

sub api_torrents_stop {
  my ( $self, $hashes ) = @_;

  # Accept: scalar ih, arrayref of ih, or "all"
  my $h = '';

  if ( !defined $hashes ) {
    return
        $self->_fail( "stop: missing hashes",
                      {ok => 0, code => 0, body => "missing hashes"} );
  }
  elsif ( ref( $hashes ) eq 'ARRAY' ) {
    my @hh = grep { defined( $_ ) && length( $_ ) } @$hashes;
    $h = join( '|', @hh );
  }
  else {
    $h = "$hashes";
  }

  $h =~ s/\s+//g;

# qBittorrent supports "all", otherwise require one-or-more 40-hex separated by |
  if ( $h ne 'all' && $h !~ /\A[0-9a-f]{40}(?:\|[0-9a-f]{40})*\z/i ) {
    return
        $self->_fail( "stop: bad hashes: [$h]",
                      {ok => 0, code => 0, body => "bad hashes: [$h]"} );
  }

  return $self->_api_post_form( '/api/v2/torrents/stop', {hashes => $h} );
}

1;

# sub _api_post_form {
#   my ( $self, $path, $form_href ) = @_;
#   die "missing path" unless defined $path && length $path;
#
#   $form_href ||= {};
#   die "form must be HASH" if ref( $form_href ) && ref( $form_href ) ne 'HASH';
#
#   my $url = "$self->{base_url}$path";
#
#   # Force deterministic x-www-form-urlencoded encoding
#   my $u = URI->new( $url );
#   $u->query_form( %$form_href );
#
#   my $req = HTTP::Request->new( POST => $u->as_string );
#   $req->header( 'Content-Type' => 'application/x-www-form-urlencoded' );
#
# # NOTE: query_form put params in URL; move them into the body (what qBt expects)
#   my $body = $u->query // '';
#   $u->query( undef );
#   $req->uri( $u );
#   $req->content( $body );
#
#   # Optional exact payload debug
#   if ( $self->{debug_http} ) {
#     warn "[QBTL::QBT] POST $path\n";
#     warn "[QBTL::QBT] url=" . $u->as_string . "\n";
#     warn "[QBTL::QBT] body=$body\n";
#   }
#
#   my $res = $self->{ua}->request( $req );
#
#   return
#       $self->_fail(
#                     "POST failed: $path: " . $res->status_line,
#                     {
#                      ok   => 0,
#                      code => ( $res->code            // 0 ),
#                      body => ( $res->decoded_content // '' ),
#                     }
#       ) unless $res->is_success;
#
#   return {
#           ok   => 1,
#           code => ( $res->code            // 200 ),
#           body => ( $res->decoded_content // '' ),};
# }
