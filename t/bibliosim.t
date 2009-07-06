#
#===============================================================================
#         FILE:  bibliosim.t
#      CREATED:  07/05/2009 09:32:45 PM
#===============================================================================

use strict;
use warnings;

use Test::More;

use lib qw(t/lib lib ../lib);
use Test::More;
use SQL::BibliosophSim;

my $bs = new SQL::BibliosophSim();
isa_ok($bs,'SQL::BibliosophSim');



is(ref($bs->USER()),'ARRAY', 'ARRAY Test query resultset' );
is(ref($bs->USER()->[0]),'ARRAY', 'ARRAY Test query row');
is(ref($bs->USER()->[0]->[0]),'', 'ARRAY Test query column');

is(ref($bs->h_USER()),'ARRAY', 'HASH Test query resultset');
is(ref($bs->h_USER()->[0]),'HASH', 'HASH Test query row');

is(ref($bs->rowh_USER()),'HASH', 'HASH ROW Test query row');

done_testing;


