#
#===============================================================================
#         FILE:  BibliosophSim.pm
#      CREATED:  07/05/2009 01:59:31 PM
#===============================================================================

=head3 SQL::BibliosophSim;

    my $bs = SQL::BibliosophSim();

    my $array_of_hashes = $bs->h_ANYQUERY();
    my $hash            = $bs->rowh_ANYQUERY();
    my $array_of_arrays = $bs->ANYQUERY();

    This is a simple class to replace SQL::Bibliosoph in unit test. This generate random data and does not need a catalog file. (Methods are handled on request with AUTOLOAD).

=cut

package SQL::BibliosophSim;

use strict;
use utf8;

use vars qw($AUTOLOAD);    

use Tie::Array::Random;
use Tie::Hash::Random;
use Switch;

sub new {
	my ($that, %args) = @_;
	my $class = ref($that) || $that;

    my $self = {};

    bless $self, $class;

    $self->{rows} = 

    return $self;
}



sub AUTOLOAD {
    my ($self) = @_;

    my $ret;

    switch ($AUTOLOAD) {
        case qr/::rowh_/  {
            my %hash;
            tie %hash, 'Tie::Hash::Random';
            $ret = \%hash;

            return $ret;
        }
        case qr/::row_/  {
            my @array;
            tie @array, 'Tie::Array::Random';
            $ret = \@array;

            return $ret;
        }
        case qr/::h_/  {
            my $ret = [];
            foreach (1..10) {
                my %hash;
                tie %hash, 'Tie::Hash::Random';
                push @$ret, \%hash;
            }
            return wantarray  ? ($ret,10) : $ret;
        }
        else {
            my $ret = [];
            foreach (1..10) {
                my @array;
                tie @array, 'Tie::Array::Random';
                push @$ret, \@array;
            }
            return wantarray  ? ($ret,10) : $ret;
        }
    }
}


1;

__END__

=head1 NAME

SQL::BibliosophSim - A SQL::Bibliosoph Tester library

=head1 VERSION

Will generate random date when you call any subrotine on it. 


