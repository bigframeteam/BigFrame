#!/usr/bin/perl -w
use Cwd;

# Simple method to print new lines
sub println {
    local $\ = "\n";
    print @_;
}

# Make sure we have all the arguments
if ($#ARGV < 1)
{
   println qq(Usage: perl $0 scale table_name);
   println qq(  scale: the scale factor of tpcds generator    );
   println qq(  table_name: the table to be generated    );
   exit(-1);
}

# Get the input data
my $SCALE_FACTOR    = $ARGV[0];
my $TABLE_NAME		= $ARGV[1];

!system qq(unzip -n dsdgen.zip) or die $!;
my $pwd = Cwd::cwd();
chdir("./dsdgen");
!system qq(make) or die $!;
!system qq(./dsdgen -scale $SCALE_FACTOR -table $TABLE_NAME -force) or die $!;
chdir($pwd);

