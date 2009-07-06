#
#===============================================================================
#         FILE:  bibliosim.t
#      CREATED:  07/05/2009 09:32:45 PM
#===============================================================================

use strict;
use warnings;

use Test::More;
use Scalar::Util qw(looks_like_number);

use lib qw(t/lib lib ../lib);

use SQL::BibliosophSim;


my $bs = new SQL::BibliosophSim();
isa_ok($bs,'SQL::BibliosophSim');



is(ref($bs->USER()),'ARRAY', 'ARRAY Test query resultset' );
is(ref($bs->USER()->[0]),'ARRAY', 'ARRAY Test query row');
is(ref($bs->USER()->[0]->[0]),'', 'ARRAY Test query column');

is(ref($bs->h_USER()),'ARRAY', 'HASH Test query resultset');
is(ref($bs->h_USER()->[0]),'HASH', 'HASH Test query row');

is(ref($bs->rowh_USER()),'HASH', 'HASH ROW Test query row');


my $a =  $bs->USER()->[0]->[0];
ok(looks_like_number $a, "Query returns a number: $a");

$a =  $bs->USER()->[8]->[4234];
ok(looks_like_number $a, "Query returns a number: $a");


my ($l,$b) =  $bs->h_USER();
ok($b, "Size is 10");
ok(ref($l) eq 'ARRAY', "IS a hash");

done_testing;


