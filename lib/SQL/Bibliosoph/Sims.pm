#
#===============================================================================
#         FILE:  Bibliosoph::Sims.pm
#      CREATED:  07/05/2009 01:59:31 PM
#===============================================================================
#
#

=head1 NAME

SQL::Bibliosoph::Sims - A SQL::Bibliosoph Tester library


=head1 SYNOPSIS

    my $bs = SQL::Bibliosoph::Sims();

    my $array_of_hashes = $bs->h_ANYQUERY();
    my $hash            = $bs->rowh_ANYQUERY();
    my $array_of_arrays = $bs->ANYQUERY();

    This is a simple class to replace SQL::Bibliosoph in unit test. This generate random data and does not need a catalog file. (Methods are handled on request with AUTOLOAD). The returned value is in concordance with the requested resulset( e.g.: If you ask for that hash (with the prefix rowh_) you will get a hashref).
        

=head1 DESCRIPTION

Will generate random date when you call any subrotine on it.  This module is inspired on Test::Sims.


=head1 Constructor parameters

=head3 rows

    This controls how many rows will be returned in the resultset. Defaults to 10.

=head3 presets 

    You can costumize the return of some particular query by using preset, like this:

    my $bs = SQL::Bibliosoph::Sims(
                    presets => {
                        rowh_USER       => { name => 'Juan', age => '42' },
                        rowh_COSTUMER   => { 
                                    name => 'rand_words( size=>10 )', 
                                    age =>  'rand_chars( size=>2 )',
                        },
                    }
    );

    If the value starts with 'rand_' the argument will be evalutated. See Data::Random documentation for details about rand_ functions.


=head1 TODO
    
    Maybe SQL::Bibliosoph::Sims should support catalogs files (.bb). This should be simple to implement using       
   

=cut

package SQL::Bibliosoph::Sims; {
    use Object::InsideOut;
    use strict;
    use utf8;
    use Carp;
    use Data::Dumper;

    use vars qw($AUTOLOAD);    

    use SQL::Bibliosoph::CatalogFile;

    use Tie::Array::Random;
    use Tie::Hash::Random;
    use Data::Random qw(:all);
    use Switch;

    my @rows    :Field :Arg(Name=>'rows', Default=>10);
    my @presets :Field :Arg(Name=>'presets', Type=> 'HASH_ref');
    my @presets_catalog :Field :Arg(Name=>'presets_catalog');

    sub _init :Init {
        my ($self) = @_;
        my $qs;


        if (my $qs = $presets[$$self]) {
            $self->create_presets($qs);
        }

        my $file = $presets_catalog[$$self];
        if ($file) {

            die $! if ! -e $file;

            my  $qs = SQL::Bibliosoph::CatalogFile->new( file => $file )->read();
            $self->create_presets($qs); 
        }

        return $self;
    }

    sub create_presets :Private {
        my ($self, $qs) = @_;

        no strict 'refs';
        no warnings 'redefine';

        foreach my $name ( keys %$qs ) {
            my $value = $qs->{$name};

            # Is this a refence?
            *{__PACKAGE__.'::'.$name} = sub {
                my ($that) = shift;
                return eval $value;
            };
        }            
    }



    sub _default :Automethod {
        my ($self) = @_;
        my @args   = @_;
        my $method_name = $_;

        # This class can handle the method directly
        my $handler = sub {

            my $self = shift;
            my $ret;
            my $rows = $rows[$$self];

            switch ($method_name) {
                case qr/\browh_/  {
                    my %hash;
                    tie %hash, 'Tie::Hash::Random';
                    $ret = \%hash;

                    return $ret;
                }
                case qr/\brow_/  {
                    my @array;
                    tie @array, 'Tie::Array::Random';
                    $ret = \@array;

                    return $ret;
                }
                case qr/\bh_/  {
                    my $ret = [];
                    foreach (1..$rows) {
                        my %hash;
                        tie %hash, 'Tie::Hash::Random';
                        push @$ret, \%hash;
                    }
                    return wantarray  ? ($ret,$rows) : $ret;
                }
                else {
                    my $ret = [];
                    foreach (1..$rows) {
                        my @array;
                        tie @array, 'Tie::Array::Random';
                        push @$ret, \@array;
                    }
                    return wantarray  ? ($ret,$rows) : $ret;
                }
            }

        };

        return ($handler);
    }
}


1;

__END__


