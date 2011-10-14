#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'DBIx::Class::InflateColumn::S3' );
}

diag( "Testing DBIx::Class::InflateColumn::S3 $DBIx::Class::InflateColumn::S3::VERSION, Perl $], $^X" );
