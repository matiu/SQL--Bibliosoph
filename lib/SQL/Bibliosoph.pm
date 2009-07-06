package SQL::Bibliosoph; {
	use Object::InsideOut;
	use strict;
    use utf8;
	use Carp;
	use Data::Dumper;
	use SQL::Bibliosoph::Query;
	use SQL::Bibliosoph::CatalogFile;

	use vars qw($VERSION );
	$VERSION = "1.6";

	our $QUIET = 0;
	our $DEBUG = 0;

	my $STD = <<"END";

--[	 LAST ]		
	SELECT LAST_INSERT_ID()

--[	FOUND ]
	SELECT FOUND_ROWS()

--[ ROWS ]
	SELECT ROW_COUNT()

--[ VERSION ]
	SELECT VERSION()

END

	my @dbh		:Field 
				:Arg(Name=> 'dbh', Mandatory=> 1) 
				:Std(dbh);

	my @delayed	:Field 
				:Arg(Name=> 'delayed', Default=> 0) 
				:Std(delayed);

	my @catalog	:Field 
				:Type(ARRAY_ref)
				:Arg(Name=> 'catalog') 
				:Std(catalog);

	my @catalog_str	:Field 
					:Arg(Name=> 'catalog_str') 
					;

	my @path	:Field 
				:Arg(Name=> 'path', Default=> '.')
				:Std(path);

	my @benchmark	
                :Field 
				:Arg(Name=> 'benchmark');

	my @constants	:Field 
					:Arg(Name=> 'constants_from')
					:Std(constants_from);

	my @constants_path	:Field 
						:Arg(Name=> 'constants_path')
						:Std(constants_path);

    my @debug	:Field 
						:Arg(Name=> 'debug')
						:Std(debug);

	my @queries	:Field;		# SQL::Bibliosoph::Query objects

	# Constuctor
	sub init :Init {
		my ($self) = @_;
		say("Constructing Bibliosoph # $$self");;
        #print STDERR Dumper( "ARGS:", $dbh[$$self], " Catalog: ", $catalog[$$self]);
        
		$self->init_all();
	}


	sub init_all :Private {
		my ($self) = @_;

        # Benchmarking enabled?? Trigger debug.
        if ($benchmark[$$self]) {
		    $SQL::Bibliosoph::Query::BENCHMARK = 1;
            $DEBUG = 1;
        }

		# propagates debug
		$SQL::Bibliosoph::CatalogFile::DEBUG = $DEBUG;
		$SQL::Bibliosoph::Query::DEBUG = $DEBUG;


		$SQL::Bibliosoph::Query::QUIET = $QUIET;
		# Start Strings
		foreach my $s ($STD, $catalog_str[$$self]) {
			$self->do_all_for(SQL::Bibliosoph::CatalogFile->_parse($s));
		}


		# Start files
		foreach my $fname (@{$catalog[$$self]}) {
			$self->do_all_for(
				SQL::Bibliosoph::CatalogFile->new(
							file 			=> $fname, 
							path 			=> $path[$$self],
				)->read()
			);
		}
	}
	
	# -------------------------------------------------------------------------
	# Extra
	# -------------------------------------------------------------------------

	sub dump {
		my ($self) = @_;

		my $str='';

		foreach (keys %{$queries[$$self]}) {
			$str .= $queries[$$self]->{$_}->dump();
		}
		return $str;
	}

	# -------------------------------------------------------------------------
	# Privates
	# -------------------------------------------------------------------------
	sub do_all_for :Private {
		my ($self,$qs) = @_;

		croak 'No Database handler at '.__PACKAGE__ if ! $dbh[$$self];

		$self->replace_contants($qs);
		$self->create_queries_from($qs);
		$self->create_methods_from($qs);
	}

	sub create_methods_from :Private {
		my ($self,$q)  = @_;

		while ( my ($name,$st) = each (%$q) ) {
			next if !$st;
			my $type;
			if ($st =~ /^\s*\(?\s*(\w+)/ ) {
				$type = $1;
			}

			# Small exception: if it is an INSERT but has ON DUPLICATE KEY
			# set as UPDATE (treated as REPLACE)
			if ($st =~ /ON\b.*DUPLICATE\b.*KEY\b.*UPDATE\b/is ) {
				$type = 'UPDATE';
			}

			# Small exception2: if it is a SELECT with SQL_CALC_FOUND_ROWS
			if ($st =~ /SQL_CALC_FOUND_ROWS/is ) {
				$type = 'SELECT_CALC';
			}

# CALL no ALWAYS returns a results set!!            
#			# Small exception3: CALL => Possible RESULT SET
#			if ($st =~ /^CALL/is ) {
#				$type = 'SELECT';
#			}

			$self->create_method_for(uc($type||''),$name);
		}


		say("\tCreated methods for [".(keys %$q)."] queries.");
	}



	
	sub create_method_for :Private {
		my ($self,$type,$name) = @_;
		$_ = $type;
		SW: {
			no strict 'refs';
			no warnings 'redefine';



			/^SELECT\b/ && do {
				# Returns
				# scalar : results
				
				# Many
				*$name = sub {
					my ($that) = shift;
					dbg_me('many ',$name,@_);
					return $queries[$$that]->{$name}->select_many([@_]);
				};

				# Many, hash
				my $name_row = 'h_'.$name;
                # Many
				*$name_row = sub {
					my ($that) = shift;
					dbg_me('manyh ',$name,@_);
					return $queries[$$that]->{$name}->select_many([@_],{});
				};

				# Row
				$name_row = 'row_'.$name;

				*$name_row = sub {
					my ($that) = shift;
					dbg_me('row  ',$name,@_);
					return $queries[$$that]->{$name}->select_row([@_]);
				};

				# Row hash
				$name_row = 'rowh_'.$name;

				*$name_row = sub {
					my ($that) = shift;
					dbg_me('rowh  ',$name,@_);
					return $queries[$$that]->{$name}->select_row_hash([@_]);
				};
				last SW;
			};


			/^SELECT_CALC\b/ && do {
				# Returns
				# scalar : results
				# array  : (results, found_rows)
				
				# Many
				*$name = sub {
					my ($that) = shift;
					dbg_me('many ',$name,@_);


					return wantarray 
						? $queries[$$that]->{$name}->select_many2([@_])
						: $queries[$$that]->{$name}->select_many([@_]) 
						;
				};

				# Many, hash
				my $nameh = 'h_'.$name;
				*$nameh = sub {
					my ($that) = shift;
					dbg_me('manyh ',$name,@_);

					return wantarray 
						? $queries[$$that]->{$name}->select_many2([@_],{})
						: $queries[$$that]->{$name}->select_many([@_],{}) 
						;
				};
	
				last SW;
			};


			/^INSERT/ && do {
				# Returns
				#  scalar :  last_insert_id 
				#  array  :  (last insert_id, row_count)

				# do
				*$name = sub {
					my ($that) = shift;
					dbg_me('inse ',$name,@_);
					
					my $ret = $queries[$$that]
								->{$name}
								->select_do([@_]);

					return 0 if $ret->rows() == -1;
								
					return wantarray 
						? ($ret->{mysql_insertid}, $ret->rows() ) 
						:  $ret->{mysql_insertid}
						;

				};
				last SW;
			};	

			if ( /^UPDATE/ ) {
				# Update has the same query than unknown
			}

			# Returns
			#  scalar :  SQL_ROWS (modified rows)
			*$name = sub {
				my ($that) = shift;
				dbg_me('oth  ',$name,@_);

				return $queries[$$that]
							->{$name}
							->select_do([@_])
							->rows();
			};
		}
	}

	#------------------------------------------------------------------
	sub replace_contants :Private {
		my ($self,$qs)  =@_;

		my $p = $constants[$$self];
		return if !$p;


		eval {
			push @INC, $constants_path[$$self] if $constants_path[$$self];

			# Read constants
			eval "require $p";
			import $p;
			my @cs = Package::Constants->list($p);

			say("\tConstants from $p [".@cs."] ");


			# DO Replace constants
			foreach my $v (values %$qs) {
				next if !$v;
				foreach my $key (@cs) {
					my $value = eval "$key" ;
					$v =~ s/\b$key\b/$value/g;
				}
			}
		};
		if ($@) {
			die "error importing constants : $@";
		}
	}


	#------------------------------------------------------------------

	sub dbg_me :Private {
		my ($what,$name,@values) =@_;
		say("$what $name "
				.   join(',', map { defined $_ ?  $_  : 'NULL' } @values ) 
		);
	}

	#------------------------------------------------------------------
	sub create_queries_from :Private {
		my ($self,$qs) = @_;
        my $i = 0;

		while ( my ($name,$st) = each (%$qs) ) {
			next if !$st;

			# Previous exists?
			if ( $queries[$$self]->{$name}  ) {
				delete $queries[$$self]->{$name};
			}

            my $args =  {
                         dbh => $dbh[$$self],
                        st  => $st, 
                        name=> $name,
                        delayed => $delayed[$$self],
            };
            #print STDERR " Query for ".Dumper($args);            

			# Prepare the statement
			$queries[$$self]->{$name} = SQL::Bibliosoph::Query->new( $args );

            $i++;                  
		}
		say("\tPrepared $i Statements". ( $delayed[$$self] ? " (delayed) " : '' ));
	}

	#------------------------------------------------------------------
	sub say {
		print STDERR "@_\n" if $DEBUG; 
	}


}



1;

__END__

=head1 NAME

SQL::Bibliosoph - A SQL Statements Library 

=head1 VERSION

1.4

=head1 SYNOPSIS

	use SQL::Bibliosoph;


    # To enable DEBUG, set:
    # $SQL::Biblosoph::DEBUG=1;

	my $bs = SQL::Biblioshoph->new(
			dsn		 => $database_handle,
			catalog  => [ qw(users products <billing) ],
    #       benchmark=> 1, # to enable statement benchmarking and debug
	);


    # To disable all debug output. 
    # $SQL::Biblosoph::QUIT=1;

	# Using dynamic generated functions.  Wrapper funtions 
	# are automaticaly created on module initialization.
	# Query should something like:

	# --[ get_products ]
	#  SELECT id,name FROM  product WHERE country = ?
	
	my $products_ref = $bs->get_products($country);

	# Forcing numbers in parameters
	# Query:

	# --[ get_products ]
	#  SELECT id,name FROM  product WHERE country = ? LIMIT #?,#?

	
	# Parameter ordering and repeating
	# Query:
	
	# --[ get_products ]
	#  SELECT id,name 
	#  		FROM  product 
	#  		WHERE 1? IS NULL OR country = 1? 
	#  		 AND  price > 2? * 0.9 AND print > 2? * 1.1
	#  		LIMIT #3?,#4?
	
	my $products_ref = $bs->get_products($country,$price,$start,$limit);

	# The same, but with an array of hashs result (add h_ at the begining)

	my $products_array_of_hash_ref = $bs->h_get_products($country,$price,$start,$limit);
	

	# Selecting only one row (add row_ at the begining)
	# Query:
	
	# --[ get_one ]
	#  SELECT name,age FROM  person where id = ?;
	
	my $product_ref = $bs->row_get_one($product_id);
	
	# Selecting only one value (same query as above)
	my $product_name = $bs->row_get_one($product_id)->[1];


	# Selecting only one row, but with HASH ref results (same query as above)
                                    (add rowh_ at the begining)
	my $product_hash_ref = $bs->rowh_get_one($product_id);
	

	# Inserting a row, with an auto_increment PK.
	# Query:
	
	# --[ insert_person ]
	#  INSERT INTO person (name,age) VALUES (?,?);
	
	my $last_insert_id = $bs->insert_person($name,$age);


	# Updating some rows
	# Query:
	
	# --[ age_persons ]
	#  UPDATE person SET age = age + 1 WHERE birthday = ?
	
	my $updated_persons = $bs->age_persons($today);



=head1 DESCRIPTION

SQL::Bibliosoph is a SQL statement library engine that allow to clearly separate SQL statements from PERL code. It is currently tested on MySQL 5.x, but it should be easly ported to other engines. 

The catalog files are prepared a the initialization, for performance reasons. The use of prepared statement also helps to prevents SQL injection attacks.  SQL::Bibliosoph supports bind parameters in statements definition and bind parements reordering. (SEE SQL::Bibliosoph::CatalogFile for details). 


All functions throw 'carp' on error. The error message is 'SQL ERROR' and the mysql error reported by the driver.

=head1 Constructor parameters

=head3 dsn

The database handler. For example:

	my $dbh = DBI->connect($dsn, ...);
	my $bb = SQL::Bibliosoph(dsn=>$dsn, ...);

=head3 catalog 
	
An array ref containg filenames with the queries. This files should use de SQL::Bibliosoft::CatalogFile format (SEE Perldoc for details). The suggested extension for these files is 'bb'. The name can be preceded with a "<" forcing the catalog the be open in "read-only" mode. In the mode, UPDATE, INSERT and REPLACE statement will be parsed. Note the calling a SQL procedure or function that actually modifies the DB is still allowed!

All the catalogs will be merged, be carefull with namespace collisions. the statement will be prepared at module constuction.

=head3 catalog_str 
	
Allows to define a SQL catalog using a string (not a file). The queries will be merged with Catalog files (if any).			
	
=head3 constants_from 

In order to use the same constants in your PERL code and your SQL modules, you can declare a module using `constants_from` paramenter. Constants exported in that module (using @EXPORT) will be replaced in all catalog file before SQL preparation.


=head3 constants_path

Define the search path for `constants_from`  PERL modules.

=head3 delayed 

Do not prepare all the statements at startup. They will be prepared individualy,  when they are used for the first time. Defaults to false(0).

=head3 benchmark

Use this to enable Query profilling. The elapsed time (in miliseconds) will be printed
to STDERR after each query execution.

=head1 Bibliosoph

n. person having deep knowledge of books. bibliognostic.

=head1 AUTHORS

SQL::Bibliosoph by Matias Alejo Garcia (matias at confronte.com) and Lucas Lain (lucas at confronte.com).

=head1 COPYRIGHT

Copyright (c) 2007 Matias Alejo Garcia. All rights reserved.
This program is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

=head1 SUPPORT / WARRANTY

The SQL::Bibliosoph is free Open Source software. IT COMES WITHOUT WARRANTY OF ANY KIND.


=head1 SEE ALSO
	
SQL::Bibliosoph::CatalogFile

At	http://nits.com.ar/bibliosoph you can find:

	* Examples
	* VIM syntax highlighting definitions for bb files
	* CTAGS examples for indexing bb files.


=head1 ACKNOWLEDGEMENTS

To Confronte.com and its associates to support the development of this module.

=head1 BUGS

This module is only tested with MySQL. Migration to other DB engines should be
simple accomplished. If you would like to use Bibliosoph with other DB, please 
let me know and we can help you if you do the testing.
	

