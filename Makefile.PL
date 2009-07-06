use ExtUtils::MakeMaker;

WriteMakefile(
	NAME            => 'SQL::Bibliosoph',
	VERSION_FROM    => 'lib/SQL/Bibliosoph.pm',
	AUTHOR			=> 'Matias Alejo Garcia ( matiu@cpan.org ) ',
	ABSTRACT		=> 'A SQL Query library',
    PREREQ_PM 	   	=> {
            'DBI' 				=> 1.5,
            'DBD::mysql' 		=> 4.004,
            'Test::More' 	    => undef,
			'Time::HiRes'       => undef,
			'Object::InsideOut' => undef,
			'Carp'				=> undef,
			'Package::Constants'=> undef,
            'Switch'            => undef,
	},
);

