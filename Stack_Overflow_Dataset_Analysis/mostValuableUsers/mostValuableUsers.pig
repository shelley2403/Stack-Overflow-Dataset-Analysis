-- mostValuableUsers.pig

loaded = LOAD 'hdfs://Shrini:9000/stackexchange/Users.txt' Using PigStorage() AS (userId:int, reputation:int, creationDate:chararray, lastAccessDate:chararray, displayName:chararray, views:int, upvote:int, downvote:int, accountId:int);
qfiltered = FILTER loaded BY NOT(lastAccessDate MATCHES '^40997844.*');
generated = FOREACH qfiltered GENERATE reputation, userId;
ordered = ORDER generated BY reputation DESC;
limited = LIMIT ordered 100;
STORE limited INTO '/pigresults/mostValuableUsers';