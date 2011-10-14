package DBIx::Class::InflateColumn::S3;

use strict;
use warnings;
use base 'DBIx::Class';
use Data::Dumper;
use Net::Amazon::S3;

our $VERSION = '0.01a';

__PACKAGE__->load_components(qw/InflateColumn/);

sub _get_content_type {
	my ( $filename, $force_app ) = @_;
	my %content_type_lookup = (
		jpg => 'image/jpg',
		jpeg => 'image/jpg',
		png => 'image/png',
		gif => 'image/gif',
		pdf => 'application/pdf',
		mp3 => 'audio/mpeg',
		mpeg => 'video/mpeg',
		mp4 => 'video/mpeg',
		mov => 'video/quicktime',
	);
	# pull out the extension FFAP
    my @file = split /\./, $filename; 
    my $extension = lc( $file[-1] );
	return $content_type_lookup{$extension} && !$force_app ? $content_type_lookup{$extension} : 'application/octet-stream';

};

sub register_column {
  my ($self, $column, $info, @rest) = @_;
  $self->next::method($column, $info, @rest);
   return unless ($info->{is_s3_column});
    $self->inflate_column(
      $column =>
        {
          inflate => sub { 
            my ($value, $obj) = @_;
            # return $self->column_data($column);
            return $value;
            
          },
          deflate => sub {
            my ($value, $obj) = @_;
            my $bucket_name = $value->{bucket} ? $value->{bucket} : $info->{bucket_name};

            #strip the info out of the insert
            my $key     	=  $value->{key};
            my $fs_path 	=  $value->{fs_path};
            my $acl_short	=  $value->{acl_short} ? $value->{acl_short} : 'private';
            my $acl_xml		=  $value->{acl_xml};
            my $meta    	=  $value->{meta};
			my $content_type =  &_get_content_type($key, $info->{force_application});

            my $s3 = Net::Amazon::S3->new(
                {
                    aws_access_key_id     => $value->{access_id},
                    aws_secret_access_key =>  $value->{secret_key}
                }
            );
            die "no sevice object" unless $s3;
            my $bucket = $s3->bucket($bucket_name);
            die "no bucket object" unless $bucket;
            die "no key object" unless $key;
            die "no file object" unless $fs_path;
	
			# TODO convert meta to a hash so the key becomes the meta suffix and the value does the right thing.. MAP MAP MAP
            my $result = $bucket->add_key_filename( $key, $fs_path, { 'x-amz-meta-api' => "ics3", 
                                                                      'x-amz-meta-idstring' => $meta, 
                                                                      'content-type' => $content_type });
            # can't trust result status 
            die "tried with $bucket_name and got ".$bucket->errstr unless $result;
			if ( $acl_xml ) {
                my $result = $bucket->set_acl({acl_xml => $acl_xml, key => $key} );
            die "tried with $acl_xml and got ".$bucket->errstr unless $result;
	
			} elsif ( $acl_short && $acl_short ne 'private'){
                $bucket->set_acl({acl_short => $acl_short, key => $key} );
            }    
            delete $value->{access_id};
            delete $value->{secret_key};
            return $bucket_name . '/' . $key;
          },
        }
    );
}


sub delete {
    my ( $self, @rest ) = @_;

    my @column_names = $self->columns;
    for (@column_names) {
        if ( $self->column_info($_)->{is_s3_column} ) {
        }
    }

    my $ret = $self->next::method(@rest);

    return $ret;
}

=head1 NAME

DBIx::Class::InflateColumn::S3 - pulls files off the FS and loads them into Amazon's Simple Storage Service using NET::AMAZON::S3

=head1 SYNOPSIS

Put this in your  L<DBIx::Class> table class:

 __PACKAGE__->add_columns(
    # your other column definitions here 
    'object',
    {
        data_type   => 's3',
        size        => 255,
        bucket_name => $target_bucket_name
    },
    # and maybe some more here
 );
    

To create a row with said S3 column:

 $schema->resultset('YourClass')->create({
    object      => { 
        #the name of the key you would like 
        key     => $key_name, 
        #the path to your file in the local FS
        fs_path => $path,      
        
        # OPTIONAL, override the bucket defined in the table class
        bucket  => $bucket,   
        # OPTIONAL, one of the recognize ACL types from the S3 docs
        acl     => $ACL # defaults to private
        # the accessID provided by amazon web services
        access_id  => $access_id,
        # the secret_key provided by amazon web services
        secret_key => $secret_key
    },
 });


Retrieving a row with the an S3 column currently only retrieves the key that was
stored in S3 in the format of $bucket_name/$key.

More fun later! 

=head1 TODO

Remove dies and raise exceptions 

Add delete methods, 

Rreturn a simple object that provides convience accessors to basic 
L<Net::Amazon::S3::Bucket> methods 

=head1 DESCRIPTION

InflateColumn::S3

=head1 AUTHOR

Emerson Mills

=head1 LICENSE

This library is free software, you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

1;
