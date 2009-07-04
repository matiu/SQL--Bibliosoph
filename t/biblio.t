# test of Exporter with DBIx::Interpolate and FILTER.
use strict;
use lib qw(t/lib lib ../lib);
use DBD::Mock;
use Test::More 'no_plan';
use SQL::Bibliosoph;

my $str = <<"END";
--[ USERS1 ]
SELECT * FROM users

--[ USERS2 ]
SELECT * FROM users WHERE id = ?

--[ USERS3 ]
SELECT * FROM users WHERE id = #1? AND name = 2?

--[ USERS3 ]
SELECT * FROM users WHERE id = #1? AND name = 2?



END

my $dbh = DBI->connect('DBI:Mock:', '', '')
    or die "Cannot create handle: $DBI::errstr\n";

my $bb = new SQL::Bibliosoph( {dbh => $dbh, catalog_str => $str  } );
is(ref($bb),'SQL::Bibliosoph','Constructor');


my $q1 = $bb->USERS1();
is(ref($q1),'ARRAY','Simple query 1');

my $q2 = $bb->USERS2();
is(ref($q2),'ARRAY','Simple query 2');


my $q3 = $bb->USERS3();
is(ref($q3),'ARRAY','Simple query 3');




