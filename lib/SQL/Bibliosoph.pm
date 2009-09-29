package SQL::Bibliosoph; {
	use Moose;

    use Carp;
	use Data::Dumper;
	use SQL::Bibliosoph::Query;
	use SQL::Bibliosoph::CatalogFile;

    our $VERSION = "2.03";


    has 'dbh'       => ( is => 'ro', isa => 'DBI::db',  required=> 1);
    has 'catalog'   => ( is => 'ro', isa => 'ArrayRef', default => sub { return [] } );
    has 'catalog_str'=>( is => 'ro', isa => 'Str');

    has 'constants_from' =>( is => 'ro', isa => 'Str');

    has 'delayed'   => ( is => 'ro', isa => 'Bool', default=> 0);
    has 'debug'     => ( is => 'ro', isa => 'Bool', default=> 0);
    has 'benchmark' => ( is => 'ro', isa => 'Num', default=> 0);

    has 'queries'   => ( is => 'rw', default=> sub { return {}; } );

    ## OLD (just for backwards compat)
    has 'path' => ( is => 'rw', isa => 'Str', default=> '');

	sub d {
        my $self = shift;
	    print STDERR join (' ', map { $_ // 'NULL'  } @_ )  if $self->debug(); 
	}

	#------------------------------------------------------------------

	sub BUILD {
		my ($self) = @_;

		$self->d( "Constructing Bibliosoph\n" ) ;

        $self->path(  $self->path() . '/' ) if $self->path() ;

		# Start Strings
        $self->do_all_for(SQL::Bibliosoph::CatalogFile->_parse($self->catalog_str()))
            if $self->catalog_str();                


		# Start files
		foreach my $fname (@{ $self->catalog() }) {
			$self->do_all_for(
				SQL::Bibliosoph::CatalogFile->new(
							file 			=>  $self->path() . $fname, 
				)->read()
			);
		}

        # $self->dbg($self->dump());
	}
	
	# -------------------------------------------------------------------------
	# Extra
	# -------------------------------------------------------------------------

	sub dump {
		my ($self) = @_;

		my $str='';

		foreach (values %{ $self->queries() }) {
			$str .= $_->dump();
		}

		return $str;
	}

	# -------------------------------------------------------------------------
	# Privates
	# -------------------------------------------------------------------------
	sub do_all_for  {
		my ($self,$qs) = @_;

		$self->replace_contants($qs);
		$self->create_queries_from($qs);
		$self->create_methods_from($qs);
	}

	sub create_methods_from {
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


		$self->d("\tCreated methods for [".(keys %$q)."] queries\n");
	}

	
	sub create_method_for {
		my ($self,$type,$name) = @_;
		$_ = $type;
		SW: {
			no strict 'refs';
			no warnings 'redefine';

            # TODO change to $self->meta->create_method();

			/^SELECT\b/ && do {
				# Returns
				# scalar : results
				
				# Many
				*$name = sub {
					my ($that) = shift;
					$self->d('many ',$name,@_);
					return $self->queries()->{$name}->select_many([@_]);
				};

				# Many, hash
				my $name_row = 'h_'.$name;
                # Many
				*$name_row = sub {
					my ($that) = shift;
					$self->d('manyh ',$name,@_);
					return $self->queries()->{$name}->select_many([@_],{});
				};

				# Row
				$name_row = 'row_'.$name;

				*$name_row = sub {
					my ($that) = shift;
					$self->d('row  ',$name,@_);
					return $self->queries()->{$name}->select_row([@_]);
				};

				# Row hash
				$name_row = 'rowh_'.$name;

				*$name_row = sub {
					my ($that) = shift;
					$self->d('rowh  ',$name,@_);
					return $self->queries()->{$name}->select_row_hash([@_]);
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
					$self->d('many ',$name,@_);


					return wantarray 
						? $self->queries()->{$name}->select_many2([@_])
						: $self->queries()->{$name}->select_many([@_]) 
						;
				};

				# Many, hash
				my $nameh = 'h_'.$name;
				*$nameh = sub {
					my ($that) = shift;
					$self->d('manyh ',$name,@_);

					return wantarray 
						? $self->queries()->{$name}->select_many2([@_],{})
						: $self->queries()->{$name}->select_many([@_],{}) 
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
					$self->d('inse ',$name,@_);
					
					my $ret = $self->queries()
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
				$self->d('oth  ',$name,@_);

				return $self->queries()
							->{$name}
							->select_do([@_])
							->rows();
			};
		}
	}

	#------------------------------------------------------------------
	sub replace_contants {
		my ($self,$qs)  =@_;

		my $p = $self->constants_from() or return;

		eval {

			# Read constants
			eval "require $p";

			import $p;
			my @cs = Package::Constants->list($p);

			$self->d("\tConstants from $p [".@cs."]\n");


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
			die "error importing constants from $p : $@";
		}
	}


	#------------------------------------------------------------------
	sub create_queries_from {
		my ($self,$qs) = @_;
        my $i = 0;

		while ( my ($name,$st) = each (%$qs) ) {
			next if !$st;

			# Previous exists?
			if ( $self->queries()->{$name}  ) {
				delete $self->queries()->{$name};
			}

            my $args =  {
                        dbh     => $self->dbh(),
                        st      => $st, 
                        name    => $name,
                        delayed => $self->delayed(),
                        debug   => $self->debug(),
                        benchmark=> $self->benchmark(),
            };
            #print STDERR " Query for ".Dumper($args);            

			# Prepare the statement
			$self->queries()->{$name} = SQL::Bibliosoph::Query->new( $args );

            $i++;                  
		}
		$self->d("\tPrepared $i Statements". ( $self->delayed() ? " (delayed) " : '' ). "\n");
	}



}



1;

__END__

=head1 NAME

SQL::Bibliosoph - A SQL Statements Library 

=head1 SYNOPSIS

	use SQL::Bibliosoph;


	my $bs = SQL::Biblioshoph->new(
			dsn		 => $database_handle,
			catalog  => [ qw(users products <billing) ],
    #       benchmark=> 0.5,      # to enable statement benchmarking and debug 
    #                               0.5 = log queries that takes more than half second
    #       debug    => 1,      # to enable debug to STDERR
	);


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

In order to use the same constants in your PERL code and your SQL modules, you can declare a module using `constants_from` paramenter. Constants exported in that module (using @EXPORT) will be replaced in all catalog file before SQL preparation. The module must be in the @INC path.


=head3 delayed 

Do not prepare all the statements at startup. They will be prepared individualy,  when they are used for the first time. Defaults to false(0).

=head3 benchmark

Use this to enable Query profilling. The elapsed time (in miliseconds) will be printed
to STDERR after each query execution, if the time is bigger that `benchmark` (must be 
given in SECONDS, can be a floating point number)

=head3 debug

To enable debug (prints each query, and arguments, very useful during development).

=head1 Bibliosoph

n. person having deep knowledge of books. bibliognostic.

=head1 AUTHORS

SQL::Bibliosoph by Matias Alejo Garcia (matiu at cpan.org) and Lucas Lain (lucas at confronte.com).

=head1 COPYRIGHT

Copyright (c) 2007-2009 Matias Alejo Garcia. All rights reserved.  This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=head1 SUPPORT / WARRANTY

The SQL::Bibliosoph is free Open Source software. IT COMES WITHOUT WARRANTY OF ANY KIND.


=head1 SEE ALSO
	
SQL::Bibliosoph::CatalogFile

At	http://nits.com.ar/bibliosoph you can find:

	* Examples
	* VIM syntax highlighting definitions for bb files
	* CTAGS examples for indexing bb files.

    You can also find the vim and ctags files in the /etc subdirectory.

    Lasted version at: http://github.com/matiu/SQL--Bibliosoph/tree/master

=head1 BUGS

This module is only tested with MySQL. Migration to other DB engines should be
simple accomplished. If you would like to use Bibliosoph with other DB, please 
let me know and we can help you if you do the testing.
	

