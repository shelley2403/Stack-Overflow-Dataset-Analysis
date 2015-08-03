-- tagcount.pig

REGISTER /Users/hdadmin/Documents/TagCountUDF.jar;
loaded = LOAD 'hdfs://Shrini:9000/stackexchange/PostQuestions.txt' Using PigStorage() AS (rowId:int, ownerUserId:int, creationDate:long, postTypeId:int, title:chararray, viewCount:float, score:float, tags:chararray, answerCount:int, commentCount:int, favouriteCount:int, closedDate:long);
generated = FOREACH loaded GENERATE FLATTEN(TagCountUDF.myclass(tags)) AS myword;;
loadedtwo = LOAD 'hdfs://Shrini:9000/stackexchange/Tags.txt' Using PigStorage() AS (tagId:int,tags:chararray);
joined = JOIN generated BY myword LEFT OUTER, loadedtwo BY tags;
filtered = FILTER joined BY (tagId is null);
STORE joined INTO '/pigresults/uncategorizedTags';