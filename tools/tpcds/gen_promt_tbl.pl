#!/usr/bin/perl -w
use Cwd;

# Simple method to print new lines
sub println {
    local $\ = "\n";
    print @_;
}

# Make sure we have all the arguments
if ($#ARGV < 0)
{
   println qq(Usage: perl $0 scale);
   println qq(  scale: the scale factor of tpcds generator    );
   exit(-1);
}

# Get the input data
my $SCALE_FACTOR    = $ARGV[0];

!system qq(unzip -n dsdgen.zip) or die $!;
my $pwd = Cwd::cwd();
chdir("./dsdgen");
!system qq(make) or die $!;
!system qq(./dsdgen -scale $SCALE_FACTOR -table promotion -force) or die $!;
chdir($pwd);

