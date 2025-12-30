package QBTL::QBT;
use common::sense;
use HTTP::Cookies;
use LWP::UserAgent;

use Mojo::JSON qw( decode_json );

use QBTL::Logger;

# ------------------------------
# Constructor / Auth
# ------------------------------

sub new {
  warn "### HIT QBTL::QBT->new ###\n";
  my ( $class, $opts ) = @_;
  $opts ||= {};

#   my ( $pkg,  $file,  $line,  $sub )  = caller( 0 );
#   my ( $pkg1, $file1, $line1, $sub1 ) = caller( 1 );
#
#   QBTL::Logger::debug(
#                        sprintf(
#                                 "[QBT.new] called_from=%s %s:%d base_url_in=%s",
#                                 ( $sub1  // '(unknown)' ),
#                                 ( $file1 // '(unknown)' ),
#                                 ( $line1 // 0 ),
#                                 (
#                                   defined $opts && ref( $opts ) eq 'HASH'
#                                   ? ( $opts->{base_url} // '(undef)' )
#                                   : '(non-hash opts)'
#                                 ) ) );
  my $self = {
              base_url     => $opts->{base_url}     || 'http://localhost:8080',
              username     => $opts->{username}     || 'admin',
              password     => $opts->{password}     || 'adminadmin',
              die_on_error => $opts->{die_on_error} || 0,
              ua => LWP::UserAgent->new( cookie_jar => HTTP::Cookies->new ),
              %$opts,};

  bless $self, $class;

  my $ok = $self->_api_auth_login;
  unless ( $ok->{ok} ) { die $ok->{err}; }
  return $self;
}

sub _api_auth_login {
  QBTL::Logger::debug( "#\t_api_auth_login" );
  my $self = shift;

  my $res = $self->{ua}->post(
                               "$self->{base_url}/api/v2/auth/login",
                               {
                                username => $self->{username},
                                password => $self->{password},
                               } );

  return
      $self->_fail(
                    "auth/login failed: " . $res->status_line,
                    {
                     ok   => 0,
                     code => ( $res->code            // 0 ),
                     body => ( $res->decoded_content // '' ),
                    }
      ) unless $res->is_success;

  return {ok => 1, code => ( $res->code // 200 ), body => ''};
}

# ------------------------------
# Helpers (validation / transport)
# ------------------------------

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

sub _fail {
  my ( $self, $msg, $out ) = @_;
  $out ||= {ok => 0};

  $out->{ok}  = 0    unless defined $out->{ok};
  $out->{err} = $msg unless defined $out->{err} && length $out->{err};

  die $msg if $self->{die_on_error};
  return $out;
}

sub _api_post_form {
  my ( $self, $path, $form_href ) = @_;
  die "missing path" unless defined $path && length $path;

  my $url = "$self->{base_url}$path";
  my $res = $self->{ua}->post( $url, $form_href || {} );

  return
      $self->_fail(
                    "POST failed: $path: " . $res->status_line,
                    {
                     ok   => 0,
                     code => ( $res->code            // 0 ),
                     body => ( $res->decoded_content // '' ),
                    }
      ) unless $res->is_success;

  return {
          ok   => 1,
          code => ( $res->code            // 200 ),
          body => ( $res->decoded_content // '' ),};
}

sub _api_get_json {
  my ( $self, $path ) = @_;
  die "missing path" unless defined $path && length $path;

  my $url = "$self->{base_url}$path";
  my $res = $self->{ua}->get( $url );

  return
      $self->_fail(
                    "GET failed: $path: " . $res->status_line,
                    {
                     ok   => 0,
                     code => ( $res->code            // 0 ),
                     body => ( $res->decoded_content // '' ),
                    }
      ) unless $res->is_success;

  my $decoded = eval { decode_json( $res->decoded_content ) };
  return
      $self->_fail(
                    "GET failed: $path: JSON decode error: $@",
                    {
                     ok   => 0,
                     code => ( $res->code            // 0 ),
                     body => ( $res->decoded_content // '' ),
                    }
      ) if $@;

  return {ok => 1, code => ( $res->code // 200 ), json => $decoded};
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

# ------------------------------
# API (Read)
# ------------------------------

sub api_torrents_info {
  my ( $self, %args ) = @_;

  my $path = "/api/v2/torrents/info";

  if ( defined $args{hashes} && length $args{hashes} ) {
    my $h = $self->_validate_hashes_pipe( $args{hashes} );
    $path .= "?hashes=$h";
  }

  my $out = $self->_api_get_json( $path );
  return $out unless $out->{ok};

  return $out->{json};    # arrayref of hashrefs
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

  my $out = $self->_api_get_json( "/api/v2/torrents/files?hash=$hash" );
  return $out unless $out->{ok};

  return $out->{json};    # arrayref of file hashes
}

sub api_torrents_infohash_map {
  my ( $self ) = @_;

  my $list = $self->api_torrents_info() || [];
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
  my ( $self, $hash, $oldPath, $newPath ) = @_;
  $self->_validate_hash( $hash );

  die "missing oldPath" unless defined $oldPath && length $oldPath;
  die "missing newPath" unless defined $newPath && length $newPath;

  my $out = $self->_api_post_form(
                                   '/api/v2/torrents/renameFolder',
                                   {
                                    hash    => $hash,
                                    oldPath => $oldPath,
                                    newPath => $newPath
                                   }, );

  return $out unless $out->{ok};

  my $files = eval { $self->api_torrents_files( $hash ) };
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

1;
