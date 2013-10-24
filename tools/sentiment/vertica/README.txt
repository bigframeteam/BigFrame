####################################################
#
#	A library implement a simple sentiment analysis
#   method by summing up the scores of all sentiment
#   words in side a string.
#	
#	Author: andy he
#   Date: Oct 23, 2013
#
####################################################

To use this library in Vertica, you need to run the 
'make install' command with proper user account. (i.e, 
the account has the privilege to run the command 'vsql',
for example, user 'dbadmin')

After install the library, we can use it with function
name 'sentiment' inside a query, for instance:

	Select id, sentiment(text) From tweet;

To uninstall this library, simply run 'make uninstall'
