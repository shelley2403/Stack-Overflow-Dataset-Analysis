-- closedQuestions.pig
REGISTER /Users/hdadmin/Documents/MilliToDateUDF.jar;
REGISTER /Users/hdadmin/Documents/TagCountUDF.jar;
loaded = LOAD 'hdfs://Shrini:9000/stackexchange/parseQuestions.txt' Using PigStorage() AS (rowId:int, ownerUserId:int, creationDate:long, postTypeId:int, title:chararray, viewCount:float, score:float, tags:chararray, answerCount:int, commentCount:int, favouriteCount:int, closedDate:chararray);
filtered = FILTER loaded BY ownerUserId != 0 and NOT(closedDate MATCHES '^40997844.*');
generator = FOREACH filtered GENERATE rowId, closedDate,FLATTEN(TagCountUDF.myclass(tags)) AS myword;
gener = FOREACH generator GENERATE myword,rowId,FLATTEN(MilliToDateUDF.MyDateMain(closedDate)) AS closeDate;
ordered = ORDER gener BY myword;
STORE ordered INTO '/pigresults/closedQuestions';