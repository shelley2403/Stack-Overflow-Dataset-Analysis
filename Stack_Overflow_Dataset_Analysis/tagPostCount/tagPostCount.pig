-- tagPostcount.pig

REGISTER /Users/hdadmin/Documents/TagCountUDF.jar;
REGISTER /Users/hdadmin/Documents/CheckDateUDF.jar;
loaded = LOAD 'hdfs://Shrini:9000/stackexchange/PostQuestions.txt' Using PigStorage() AS (rowId:int, ownerUserId:int, creationDate:chararray, postTypeId:int, title:chararray, viewCount:float, score:float, tags:chararray, answerCount:int, commentCount:int, favouriteCount:int, closedDate:long);
dated = FOREACH loaded GENERATE FLATTEN(CheckDateUDF.CheckDateUDF(creationDate)) AS datefilter,rowId,tags;
generated = FOREACH dated GENERATE FLATTEN(TagCountUDF.myclass(tags)) AS myword,datefilter,rowId;
grouped = GROUP generated BY myword;
counted = FOREACH grouped GENERATE COUNT($1.rowId) AS postCount, group;
ordered = ORDER counted BY postCount DESC;
STORE ordered INTO '/pigresults/TagPostCount';