--pageRank.pig

REGISTER /Users/hdadmin/Documents/PageRankUDF.jar;
loaded = LOAD 'hdfs://Shrini:9000/pagerank/part-m-00225' Using PigStorage() AS (rowId:int, postId:int, creationDate:chararray, relatedPostId:int);
generated = FOREACH (GROUP loaded ALL) {GENERATE FLATTEN(PageRankUDF.PageRankUDF($1.postId,$1.relatedPostId));}
STORE generated INTO '/pigresults/pageRank';