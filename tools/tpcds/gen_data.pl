#!/usr/bin/perl -w

###############################################################################
# This script can be used to generate TPCDS data in a distributed
# fashion and load them into HDFS.
#
# Usage:
#  perl gen_data.pl scale_factor num_files zipf_factor host_list local_dir hdfs_dir
#  
#  where:
#    scale_factor = TPCDS Scale factor (GB of data to generate)
#    num_files    = The number of files to generate for each table
#    host_list    = File containing a list of host machines
#    local_dir    = Local directory to use in the host machines
#    hdfs_dir     = HDFS directory to store the generated data
#
# Assumptions/Requirements:
# 1. The enviromental variable $HADOOP_HOME is defined in the master node and
#    all the slave nodes and it is the same in all nodes.
# 2. The local directory does not exist in the slave nodes
# 3. The HDFS directory does not exist
# 4. There is enough local disk space on the slave nodes to generate the data
# 5. The number of files must be greater than half the scale factor to ensure
#    that we don't try to generate a file that is greater than 2GB
#
# The data is loaded into HDFS. The name for each file is of the form
# "tablename.tbl.x", where tablename is lineitem, orders etc, and 
# x is a number between 1 and <num_files>.
# Each table is placed in a corresponding directory under <hdfs_dir>.
#
# Author: Andy He
# Date: June 09, 2013
#
##############################################################################

# Simple method to print new lines
sub println {
    local $\ = "\n";
    print @_;
}

# Make sure we have all the arguments
if ($#ARGV < 4)
{
   println qq(Usage: perl $0 scale_factor num_files host_list local_dir hdfs_dir);
   println qq(  scale_factor: TPCDS Scale factor \(GB of data to generate\));
   println qq(  num_files:    The number of files to generate for each table);
   println qq(  host_list:    File containing a list of host machines);
   println qq(  local_dir:    Local directory to use in the host machines);
   println qq(  hdfs_dir:     HDFS directory to store the generated data);
   exit(-1);
}

# Get the input data
my $SCALE_FACTOR    = $ARGV[0];
my $NUM_FILE_SPLITS = $ARGV[1];
my $HOST_LIST       = $ARGV[2];
my $LOCAL_DIR       = $ARGV[3];
my $HDFS_DIR        = $ARGV[4];


# Start data generation
println qq(Starting data generation at: ) . `date`;
println qq(Input Parameters:);
println qq(  Scale Factor:    $SCALE_FACTOR);
println qq(  Number of Files: $NUM_FILE_SPLITS);
println qq(  Host List:       $HOST_LIST);
println qq(  Local Directory: $LOCAL_DIR);
println qq(  HDFS Directory:  $HDFS_DIR);
println qq();

# Error checking
if ($SCALE_FACTOR <= 0)
{
   println qq(ERROR: The scale factor must be greater than 0);
   exit(-1);
}

if ($NUM_FILE_SPLITS < $SCALE_FACTOR / 2)
{
   println qq(ERROR: The number of files must be greater than half the scale factor);
   exit(-1);
}


if (!-e $HOST_LIST)
{
   println qq(ERROR: The file '$HOST_LIST' does not exist);
   exit(-1);
}

if (!$ENV{'HADOOP_HOME'})
{
   println qq(ERROR: \$HADOOP_HOME is not defined);
   exit(-1);
}

# Execute the hadoop-env.sh script for environmental variable definitions
!system qq(. \$HADOOP_HOME/etc/hadoop/hadoop-env.sh) or die $!;
my $HADOOP_HOME = $ENV{'HADOOP_HOME'};
my $ssh_opts = ($ENV{'HADOOP_SSH_OPTS'}) ? $ENV{'HADOOP_SSH_OPTS'} : "";


# Get the hosts
open INFILE, "<", $HOST_LIST;
my @hosts = ();
while ($line = <INFILE>)
{
   $line =~ s/(^\s+)|(\s+$)//g;
   push(@hosts, $line) if $line =~ /\S/
}
close INFILE;

# Make sure we have some hosts
my $num_hosts = scalar(@hosts);
if ($num_hosts <= 0)
{
   println qq(ERROR: No hosts were found in '$HOST_LIST');
   exit(-1);
}

# Create all the HDFS directories
$status = system qq($HADOOP_HOME/bin/hadoop fs -test -e $HDFS_DIR 2>&1);
if ($status == 0) {
   println qq(ERROR: The hdfs directory '$HDFS_DIR' already exists);
   exit(-1);
}

println qq(Creating all the HDFS directories);
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/call_center) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/catalog_page) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/catalog_returns) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/catalog_sales) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/customer_address) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/customer) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/customer_demographics) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/date_dim) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/dbgen_version) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/household_demographics) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/income_band) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/inventory) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/item) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/promotion) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/reason) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/ship_mode) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/store) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/store_returns) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/store_sales) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/time_dim) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/warehouse) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/web_page) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/web_returns) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/web_sales) or die $!;
!system qq($HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_DIR/web_site) or die $!;
println qq();

# Create the execution script that will be sent to the hosts
open OUTFILE, ">", "gen_and_load.sh" or die $!;
print OUTFILE qq(unzip -n tpcds_data_gen.zip\n);
print OUTFILE qq(perl tpcds_gen_data.pl data.properties\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/call_center*.dat $HDFS_DIR/call_center\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/catalog_page*.dat $HDFS_DIR/catalog_page\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/catalog_returns*.dat $HDFS_DIR/catalog_returns\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/catalog_sales*.dat $HDFS_DIR/catalog_sales\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/customer_address*.dat $HDFS_DIR/customer_address\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/customer_demographics*.dat $HDFS_DIR/customer_demographics\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/customer_[0-9]*.dat $HDFS_DIR/customer\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/date_dim*.dat $HDFS_DIR/date_dim\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/dbgen_version*.dat $HDFS_DIR/dbgen_version\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/household_demographics*.dat $HDFS_DIR/household_demographics\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/income_band*.dat $HDFS_DIR/income_band\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/inventory*.dat $HDFS_DIR/inventory\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/item*.dat $HDFS_DIR/item\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/promotion*.dat $HDFS_DIR/promotion\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/reason*.dat $HDFS_DIR/reason\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/ship_mode*.dat $HDFS_DIR/ship_mode\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/store_returns*.dat $HDFS_DIR/store_returns\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/store_sales*.dat $HDFS_DIR/store_sales\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/store_[0-9]*.dat $HDFS_DIR/store\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/time_dim*.dat $HDFS_DIR/time_dim\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/warehouse*.dat $HDFS_DIR/warehouse\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/web_page*.dat $HDFS_DIR/web_page\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/web_returns*.dat $HDFS_DIR/web_returns\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/web_sales*.dat $HDFS_DIR/web_sales\n);
print OUTFILE qq($HADOOP_HOME/bin/hadoop fs -put data/web_site*.dat $HDFS_DIR/web_site\n);
print OUTFILE qq(rm -rf data/*.dat\n);
close OUTFILE;
chmod 0744, "gen_and_load.sh";

# Each host will generate a certain range of the file splits
my $num_splits_per_host = int($NUM_FILE_SPLITS / $num_hosts);
$num_splits_per_host = 1 if $num_splits_per_host < 1;
my $first_file_split = 1;

# Connect to each host and generate the data
for ($host = 0; $host < $num_hosts; $host++)
{
   # Calculate the last file split generated by this host
   $last_file_split = ($host == $num_hosts-1) 
                      ? $NUM_FILE_SPLITS 
                      : $first_file_split + $num_splits_per_host - 1;
   
   # Create the data.properties file and copy it to the host
   open OUTFILE, ">", "data.properties" or die $!;
   print OUTFILE qq(host_name = $hosts[$host] \n);
   print OUTFILE qq(scaling_factor = $SCALE_FACTOR \n);
   print OUTFILE qq(num_file_splits = $NUM_FILE_SPLITS \n);
   print OUTFILE qq(first_file_split = $first_file_split \n);
   print OUTFILE qq(last_file_split = $last_file_split \n);
   print OUTFILE qq(tpcds_home = $LOCAL_DIR/data \n);
   close OUTFILE;

   # Copy the necessary files to the host
   println qq(Sending files to host: $hosts[$host]);
   !system qq(ssh $ssh_opts $hosts[$host] \"mkdir $LOCAL_DIR\") or die $!;
   !system qq(scp gen_and_load.sh   $hosts[$host]:$LOCAL_DIR/.) or die $!;
   !system qq(scp tpcds_data_gen.zip $hosts[$host]:$LOCAL_DIR/.) or die $!;
   !system qq(scp data.properties   $hosts[$host]:$LOCAL_DIR/.) or die $!;

   # Start the data generation in a child process
   println qq(Starting data generation at host: $hosts[$host]\n);
   unless (fork)
   {
      system qq(ssh $ssh_opts $hosts[$host] ).
             qq(\"cd $LOCAL_DIR; ./gen_and_load.sh >> gen_and_load.out 2>&1\");
      println qq(Data generation completed at host: $hosts[$host]\n);
      system qq(ssh $ssh_opts $hosts[$host] ).
             qq(\"cd ..; rm -r $LOCAL_DIR\");
      exit(0);
   }

   # Exit the loop if we have generated all file splits
   $first_file_split = $last_file_split + 1;
   last if $last_file_split == $NUM_FILE_SPLITS;
}

# Wait for the hosts to complete
println qq(Waiting for the data generation to complete);
for ($host = 0; $host < $num_hosts; $host++)
{
   wait;
}

# Clean up
system qq(rm data.properties);
system qq(rm gen_and_load.sh);

# Done
$time = time - $^T;
println qq();
println qq(Data generation is complete!);
println qq(Time taken (sec):\t$time);

