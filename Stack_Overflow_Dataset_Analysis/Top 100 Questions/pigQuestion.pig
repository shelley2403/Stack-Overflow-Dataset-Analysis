-- top100Questions.pig

loaded =LOAD 'hdfs://Shrini:9000/stackexchange/PostQuestions.txt' Using PigStorage() AS (rowId:int, ownerUserId:int, creationDate:long, postTypeId:int, title:chararray, viewCount:float, score:float, tags:chararray);
filtered = FILTER loaded BY score>=1;
grouped = GROUP filtered ALL;
avg = FOREACH grouped GENERATE AVG(filtered.viewCount) AS ave;
scorebyview = FOREACH filtered GENERATE rowId,ROUND((score / viewCount) * avg.ave) AS maxscore,title; 
ordered = ORDER scorebyview BY maxscore DESC;
top100 = LIMIT ordered 100;
STORE top100 INTO '/pigresults/top100Q' USING PigStorage(',');