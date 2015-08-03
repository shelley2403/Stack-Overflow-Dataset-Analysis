-- mahoutAnswers.pig
 
REGISTER /Users/hdadmin/Documents/ScaleScoreUDF.jar;
loaded = LOAD 'hdfs://Shrini:9000/stackexchange/FinalPostAnswers.txt' Using PigStorage() AS (rowId:int, ownerUserId:int, creationDate:long, postTypeId:int, score:int, commentCount:int, parentId:int);
filtered = FILTER loaded BY ownerUserId != 0 and score>0 and score<=100;
scoring = FOREACH filtered GENERATE ownerUserId,parentId,FLATTEN(Scale.Scale(score)) AS points;
STORE scoring INTO '/pigresults/mahoutAnswers';