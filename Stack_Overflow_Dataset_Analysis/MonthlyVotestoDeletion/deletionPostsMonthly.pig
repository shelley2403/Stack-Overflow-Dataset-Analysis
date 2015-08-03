-- deletionPostsMonthly.pig
REGISTER /Users/hdadmin/Documents/CountDeletionUDF.jar;
REGISTER /Users/hdadmin/Documents/MyTokenizer.jar;
loaded = LOAD 'hdfs://Shrini:9000/stackexchange/Votes.txt' Using PigStorage() AS (rowId:int, postId:int, creationDate:chararray,voteTypeId:int, userId:int);
filtered = FILTER loaded BY voteTypeId ==10;
grouped = GROUP filtered ALL;
generated = FOREACH grouped GENERATE FLATTEN(CountDeletionUDF.CountDeletionUDF($1.creationDate)) AS date;
generated = FOREACH generated GENERATE FLATTEN(Tokenizer.MyTokenize(date));

STORE generated INTO '/pigresults/deletionPostsMonthly' ;