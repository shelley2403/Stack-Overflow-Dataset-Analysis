-- CommentToAnswer.pig

REGISTER /Users/hdadmin/Documents/MyAvgUdf.jar;
loaded = LOAD 'hdfs://Shrini:9000/stackexchange/Comments.txt' Using PigStorage() AS (rowId:int, UserId:int, creationDate:chararray,postId:int,score:float);
filtered = FILTER loaded BY score>=10;
grouped = GROUP filtered ALL;
counted = FOREACH grouped GENERATE group,FLATTEN(MyAvgUdf.MyAvg($1.score)) AS myavg;
filterd = FILTER loaded BY score>=counted.myavg;
generation = FOREACH filterd GENERATE rowId,score;
ordered = ORDER generation BY score DESC;
STORE ordered INTO 'pigresults/CommentToAnswer' USING PigStorage(',');