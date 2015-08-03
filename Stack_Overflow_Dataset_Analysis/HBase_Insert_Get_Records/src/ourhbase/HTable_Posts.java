package ourhbase;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class HTable_Posts {


static String tableName = "PostQuestions";
static String colFamilyNames = "postDescription";
static HTable table;

static void createTable(String tableName, String colFams, Configuration conf)
throws IOException {

HBaseAdmin hbase = new HBaseAdmin(conf);
HTableDescriptor desc = new HTableDescriptor(tableName);
HColumnDescriptor meta = new HColumnDescriptor(colFams.getBytes());
desc.addFamily(meta);
hbase.createTable(desc);
table = new HTable(conf, tableName);
}

public static Job configureJob(Configuration conf, String[] args)
throws IOException, ClassNotFoundException, InterruptedException {
Path inputPath = new Path("hdfs://Shrini:9000/stackexchange/stackexchange/PostQuestions.txt");

createTable(tableName, colFamilyNames, conf);
Job job = new Job(conf, "myjob");
job.setJarByClass(MyHBaseMainClass.class);
FileInputFormat.setInputPaths(job, inputPath);
job.setInputFormatClass(TextInputFormat.class);
job.setMapperClass(MyMapper.class);
TableMapReduceUtil.initTableReducerJob(tableName, null, job);
job.setNumReduceTasks(0);
job.waitForCompletion(true);
return job;
}

static class MyMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
private byte[] rawUpdateColumnFamily = Bytes.toBytes(colFamilyNames);
private byte[] OwnerUserId = Bytes.toBytes("OwnerUserId");
private byte[] CreationDate = Bytes.toBytes("CreationDate");
private byte[] PostTypeId = Bytes.toBytes("PostTypeId");
private byte[] Title = Bytes.toBytes("Title");
private byte[] ViewCount = Bytes.toBytes("ViewCount");
private byte[] AnswerCount = Bytes.toBytes("AnswerCount");
private byte[] CommentCount = Bytes.toBytes("CommentCount");
private byte[] FavouriteCount = Bytes.toBytes("FavouriteCount");
private byte[] ClosedDate = Bytes.toBytes("ClosedDate");


private long statuspoint = 100;
private long count = 0;

@Override
public void map(LongWritable key, Text line, Context context)
throws IOException {


StringTokenizer stringTokenizer = new StringTokenizer(line.toString(), "\t");
byte[] row = Bytes.toBytes(stringTokenizer.nextToken());
System.out.println(row);
Put put = new Put(row);

put.add(rawUpdateColumnFamily,OwnerUserId,stringTokenizer.nextToken().getBytes());
put.add(rawUpdateColumnFamily,CreationDate,stringTokenizer.nextToken().getBytes());
put.add(rawUpdateColumnFamily,PostTypeId,stringTokenizer.nextToken().getBytes());
put.add(rawUpdateColumnFamily,Title,stringTokenizer.nextToken().getBytes());
put.add(rawUpdateColumnFamily,ViewCount,stringTokenizer.nextToken().getBytes());
put.add(rawUpdateColumnFamily,AnswerCount,stringTokenizer.nextToken().getBytes());
put.add(rawUpdateColumnFamily,CommentCount,stringTokenizer.nextToken().getBytes());
put.add(rawUpdateColumnFamily,FavouriteCount,stringTokenizer.nextToken().getBytes());
put.add(rawUpdateColumnFamily,ClosedDate,stringTokenizer.nextToken().getBytes());

System.out.println(put);

try {
context.write(new ImmutableBytesWritable(row), put);
} catch (InterruptedException e) {
e.printStackTrace();
}
if (++count % statuspoint == 0) {
context.setStatus("Emitting Put " + count);
}



}// map
}

}