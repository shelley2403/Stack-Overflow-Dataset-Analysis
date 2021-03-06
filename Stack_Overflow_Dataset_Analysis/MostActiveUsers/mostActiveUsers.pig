-- mostActiveUsers
loaded = LOAD 'hdfs://Shrini:9000/stackexchange/Users.txt' Using PigStorage() AS (userId:int, reputation:int, creationDate:chararray, lastAccessDate:chararray, displayName:chararray, views:int, upvote:int, downvote:int, accountId:int);
qfiltered = FILTER loaded BY NOT(lastAccessDate MATCHES '^40997844.*');
generated = FOREACH qfiltered GENERATE views, userId;
ordered = ORDER generated BY views DESC;
limited = LIMIT ordered 100;
STORE limited INTO '/pigresults/mostViewedUsers';