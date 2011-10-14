use strict;
use warnings;

use inc::Module::Install 0.65;

name 'DBIx-Class-InflateColumn-S3';
license 'artistic';
all_from 'lib/DBIx/Class/InflateColumn/S3.pm';

requires 'DBIx::Class';
requires 'Net::Amazon::S3';


auto_install;
WriteAll;
