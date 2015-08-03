-- mahoutLink.pig
 
loaded1 = LOAD 'hdfs://Shrini:9000/pigresults/mahoutQuestions/part-r-00000' Using PigStorage() AS (tagId:int,tags:chararray, postId:int);
loaded2 = LOAD 'hdfs://Shrini:9000/pigresults/mahoutAnswers/part-m-00000' Using PigStorage() AS (ownerUserId:int, parentId:int, score:double);

joined = JOIN loaded1 BY postId,loaded2 BY parentId;
generated = FOREACH joined GENERATE ownerUserId,tagId,score;
grouped = GROUP generated BY ownerUserId;
STORE grouped into '/test/mahout';