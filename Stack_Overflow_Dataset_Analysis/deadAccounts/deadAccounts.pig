-- deadAccounts.pig

REGISTER /Users/hdadmin/Documents/CheckAccountActivity.jar;
loaded = LOAD 'hdfs://Shrini:9000/stackexchange/Users.txt' Using PigStorage() AS (userId:int, reputation:int, creationDate:chararray, lastAccessDate:chararray, displayName:chararray, views:int, upvote:int, downvote:int, accountId:int);
qfiltered = FILTER loaded BY NOT(lastAccessDate MATCHES '^40997844.*');

generated = FOREACH qfiltered GENERATE FLATTEN(CheckAccountActivity.CheckAccountActivity(lastAccessDate)) AS date,userId,displayName;

STORE generated INTO 'pigresults/deadAccounts';

