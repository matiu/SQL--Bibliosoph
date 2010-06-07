use ExtUtils::MakeMaker;

use 5.10.0;

WriteMakefile(
	NAME            => 'SQL::Bibliosoph',
	VERSION_FROM    => 'lib/SQL/Bibliosoph.pm',
	AUTHOR			=> 'Matias Alejo Garcia ( matiu@cpan.org ) ',
	ABSTRACT		=> 'A SQL Query library',
    PREREQ_PM 	   	=> {
            'DBI' 				=> 1.5,
            'DBD::mysql' 		=> 4.004,
            'Test::More' 	    => 0.88,
            'Tie::Array::Random'=> 1.0,
            'Tie::Hash::Random' => 1.0,
			'Moose'             => 0.82,
            'DBD::Mock'         => undef,
			'Time::HiRes'       => undef,
			'Carp'				=> undef,
			'Package::Constants'=> undef,
            'Devel::Cover'      => undef,
            'Digest::MD5'       => 2.39,
            'Cache::Memcached::Fast' => 0.17,
        },
    MIN_PERL_VERSION => '5.10.0',
);

