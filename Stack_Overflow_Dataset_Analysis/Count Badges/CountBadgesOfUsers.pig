A = LOAD 'hdfs://Shrini:9000/stackexchange/PostQuestions.txt' Using PigStorage() AS (rowId:int, UserId:int, badgeName:chararray, Date:long)

B = GROUP A BY badgeName;

C = FOREACH B GENERATE group,COUNT($1.userId) AS user;

D = ORDER C BY user DESC;                             

STORE D into '/project/stackexchange/output2';