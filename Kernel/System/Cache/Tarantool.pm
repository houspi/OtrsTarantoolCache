# --
# houspi@gmail.com
# --
# This software comes with ABSOLUTELY NO WARRANTY. For details, see
# the enclosed file COPYING for license information (GPL). If you
# did not receive this file, see https://www.gnu.org/licenses/gpl-3.0.txt.
# --

package Kernel::System::Cache::Tarantool;

use strict;
use warnings;

use Kernel::System::VariableCheck qw(IsHashRefWithData);

use Encode;
use MIME::Base64;
use DR::Tarantool qw( :all );
use DR::Tarantool::MsgPack::SyncClient;
use IO::Compress::Zip qw(zip $ZipError);
use IO::Uncompress::Unzip qw(unzip  $UnzipError);

our @ObjectDependencies = (
    'Kernel::Config',
    'Kernel::System::Log',
    'Kernel::System::Main',
    'Kernel::System::Storable',
);

=head1 NAME

Kernel::System::Cache::Tarantool - module

=head1 DESCRIPTION

This is a class for store data cache in the Tarantool database

=cut

sub new {
    my ( $Type, %Param ) = @_;

    my $Self = {};
    bless( $Self, $Type );

    $Self->{StorableObject} = $Kernel::OM->Get('Kernel::System::Storable');

    my $ConfigObject   = $Kernel::OM->Get('Kernel::Config');
    $Self->{ZipLargeObject} = $ConfigObject->Get('Cache::Tarantool::ZipLargeObject');
    $Self->{SizeToCompress} = $ConfigObject->Get('Cache::Tarantool::SizeToCompress');

    $Self->{SpaceName} = $ConfigObject->Get('Cache::Tarantool::SpaceName');
    die "Cache::Tarantool::SpaceName nust be set!" if !$Self->{SpaceName};

    $Self->{MasterHost} = $ConfigObject->Get('Cache::Tarantool::MasterHost');
    die "Cache::Tarantool::MasterHost nust be set!" if !IsHashRefWithData( $Self->{MasterHost} );

    $Self->{SlaveHost} = $ConfigObject->Get('Cache::Tarantool::SlaveHost')  // $Self->{MasterHost};

    $Self->{Spaces} = {
            512 => {                                        # 512 - Magick key
                name => $Self->{SpaceName},
                fields => [
                    { name => 'key',     type => 'STRING'},
                    { name => 'type',    type => 'STRING'},
                    { name => 'value',   type => 'STRING'},
                    { name => 'expired', type => 'INT'},
                ],
                indexes => {
                    0 => {
                        name => 'primary',
                        fields => ['key', 'type' ],
                    },
                    1 => {
                        name => 'type_idx',
                        fields => ['type'],
                    },
                    2 => {
                        name => 'expired_idx',
                        fields => ['expired'],
                    },
                },
            },
    };

    $Self->{MasterTnt} = undef;
    $Self->{SlaveTnt}  = undef;

    return $Self;
}

sub Set {
    my ( $Self, %Param ) = @_;

    for my $Needed (qw(Type Key Value TTL)) {
        if ( !defined $Param{$Needed} ) {
            $Kernel::OM->Get('Kernel::System::Log')->Log(
                Priority => 'error',
                Message  => "Need $Needed!"
            );
            return;
        }
    }
    return if !$Self->{MasterTnt} && !$Self->_Connect( ConfigName => 'MasterHost', ConnectName => 'MasterTnt' );

    my $StorableString = $Self->{StorableObject}->Serialize(
        Data => {
            Value => $Param{Value},
        },
    );

    if (
        $Self->{ZipLargeObject}
        && $Self->{SizeToCompress}
        && length($StorableString) > $Self->{SizeToCompress}
       )
    {
        my $ZippedString;
        zip \$StorableString => \$ZippedString;
        if ( $ZipError ) {
            $Kernel::OM->Get('Kernel::System::Log')->Log(
                Priority => 'error',
                Message  => "Zip: $ZipError!"
            );
        }
        else {
            $StorableString = $ZippedString;
        }
    }

    $StorableString = encode_base64($StorableString);
    my $TTL = int(time()) + int($Param{TTL});

    eval {
        $Self->{MasterTnt}->replace(
            $Self->{SpaceName} => [
                             "$Param{Key}",
                             "$Param{Type}",
                             $StorableString,
                             $TTL,
                             
            ]
        );
    };
   
    if ( $@ ) {
        $Kernel::OM->Get('Kernel::System::Log')->Log(
            Priority => 'error',
            Message  => "Cache set error: $@!"
        );
        return;
    }

    return 1;
}

sub Get {
    my ( $Self, %Param ) = @_;

    for my $Needed (qw(Type Key)) {
        if ( !defined $Param{$Needed} ) {
            $Kernel::OM->Get('Kernel::System::Log')->Log(
                Priority => 'error',
                Message  => "Need $Needed!"
            );
            return;
        }
    }
    return if !$Self->{SlaveTnt} && !$Self->_Connect( ConfigName => 'SlaveHost', ConnectName => 'SlaveTnt' );

    my $Value;
    my $Row = $Self->{SlaveTnt}->select($Self->{SpaceName}, 'primary', [ "$Param{Key}", "$Param{Type}" ] );
    if ( $Row && $Row->can('value') && $Row->can('expired')  ) {
        if ( $Row->expired >= int(time()) ) {
            my $StorableString = decode_base64( $Row->value );
            my $hdr      = substr $StorableString, 0, 4;
            my $ZIPMagic = unpack("V", reverse $hdr);
            if ( $ZIPMagic == 0x504b0304 ) {
                my $UnzippedString;
                unzip \$StorableString => \$UnzippedString;
                if ( $UnzipError ) {
                    $Kernel::OM->Get('Kernel::System::Log')->Log(
                        Priority => 'error',
                        Message  => "Zip: $UnzipError!"
                    );
                }
                else {
                    $StorableString = $UnzippedString;
                }
            }
            $StorableString = $Self->{StorableObject}->Deserialize(
                Data => $StorableString
            );
            $Value = $StorableString->{Value};
        }
        else {
            $Self->Delete( Key=>$Param{Key}, Type=>$Param{Type});
        }
    }
    return $Value;
}

sub Delete {
    my ( $Self, %Param ) = @_;

    for my $Needed (qw(Type Key)) {
        if ( !defined $Param{$Needed} ) {
            $Kernel::OM->Get('Kernel::System::Log')->Log(
                Priority => 'error',
                Message  => "Need $Needed!"
            );
            return;
        }
    }
    return if !$Self->{MasterTnt} && !$Self->_Connect( ConfigName => 'MasterHost', ConnectName => 'MasterTnt' );

    $Self->{MasterTnt}->delete( $Self->{SpaceName}, [ "$Param{Key}", "$Param{Type}" ] );
    return 1;
}

sub CleanUp {
    my ( $Self, %Param ) = @_;

    return if !$Self->{MasterTnt} && !$Self->_Connect( ConfigName => 'MasterHost', ConnectName => 'MasterTnt' );

    if ( $Param{Expired} ) {
        my $ExpiredTTL = int(time());
        $Self->{MasterTnt}->call_lua('CleanUpExpiredCache' => [ $ExpiredTTL ]);
    }
    else {
        $Self->{MasterTnt}->call_lua('CleanUpCache' => [ $Param{Type} ]);
    }

    return 1;
}

sub _Connect {
    my ( $Self, %Param ) = @_;

    return if !$Param{ConfigName} || !$Param{ConnectName};

    HOST:
    foreach my $Host ( @{$Self->{$Param{ConfigName}}->{Hosts}} ) {
        eval {
            $Self->{$Param{ConnectName}} = DR::Tarantool::MsgPack::SyncClient->connect(
                host     => $Host,
                port     => $Self->{$Param{ConfigName}}->{Port},
                user     => $Self->{$Param{ConfigName}}->{User},
                password => $Self->{$Param{ConfigName}}->{Password},
                spaces   => $Self->{Spaces},
            );
        };
        last HOST if $Self->{$Param{ConnectName}};
    }
    if ( !$Self->{$Param{ConnectName}} ) {
        $Kernel::OM->Get('Kernel::System::Log')->Log(
            Priority => 'error',
            Message  => "Tarantool '$Param{ConfigName}' error: $@"
        );
        return
    }
    
    return 1;
}

1;

=head1 TERMS AND CONDITIONS

This software is part of the OTRS project (L<https://otrs.org/>).

This software comes with ABSOLUTELY NO WARRANTY. For details, see
the enclosed file COPYING for license information (GPL). If you
did not receive this file, see L<https://www.gnu.org/licenses/gpl-3.0.txt>.

=cut
