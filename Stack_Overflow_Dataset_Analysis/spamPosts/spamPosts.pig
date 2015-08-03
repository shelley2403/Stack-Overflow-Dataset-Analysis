-- spamPosts.pig

loaded = LOAD 'hdfs://Shrini:9000/stackexchange/Votes.txt' Using PigStorage() AS (rowId:int, postId:int, creationDate:chararray,voteTypeId:int,userId:int);
filtered = FILTER loaded BY voteTypeId ==12;
grouped = GROUP filtered ALL;
counted = FOREACH grouped GENERATE group,COUNT($1.rowId) AS spamCount;
loadedtwo = LOAD 'hdfs://Shrini:9000/stackexchange/Comments.txt' Using PigStorage() AS (rowId:int, UserId:int, creationDate:chararray,postId:int,score:float);
grouping = GROUP loadedtwo ALL;
counting = FOREACH grouping GENERATE group,COUNT($1.rowId) AS commentCount,counted.spamCount;
STORE counting INTO '/mypigresults/spam' ;