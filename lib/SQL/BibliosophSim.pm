#
#===============================================================================
#         FILE:  BibliosophSim.pm
#      CREATED:  07/05/2009 01:59:31 PM
#===============================================================================


package SQL::BibliosophSim;

use strict;
use utf8;

use vars qw($AUTOLOAD);    

sub new {
	my ($that, %args) = @_;
	my $class = ref($that) || $that;

    my $self = {};

    bless $self, $class;

    return $self;
}



sub AUTOLOAD {
    my ($self) = @_;
    my $function = $AUTOLOAD;

    my $a = { a=>1 , b=>2};
    my $b = [1,2];
    my $d = [{ a=>1 , b=>2}, { c=>1 , d=>2} ];
    my $c = [ [1,2], [3,4] ];

    return $a if ( $function =~ /::rowh_/ ) ;
    return $b if ( $function =~ /::row_/  ) ;
    return $d if ( $function =~ /::h_/  ) ;

    return $c;
}


1;

__END__

=head1 NAME

SQL::BibliosophSim - A SQL::Bibliosoph Tester library

=head1 VERSION

Will generate random date when you call any subrotine on it. 


