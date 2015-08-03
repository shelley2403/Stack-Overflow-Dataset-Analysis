package ourhbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyHBaseMainClass {

static HTable table;
static Configuration conf;
static Job job;

public static void main(String[] args) throws Exception {
System.out.println("Please wait while the records are being inserted into their respective HTables");

conf = HBaseConfiguration.create();

String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
job = HTable_Posts.configureJob(conf, otherArgs);
/*
HTable t = new HTable(conf, "Posts");
Scan s = new Scan();
s.setStartRow(Bytes.toBytes("OwnerUserId"));
s.setStopRow(Bytes.toBytes("OwnerUserId"));
Filter f = new ColumnRangeFilter(Bytes.toBytes("5"), true,Bytes.toBytes("51"), false);
s.setFilter(f);
s.setBatch(20); // avoid getting all columns for the HBase row 
ResultScanner rs = t.getScanner(s);
for (Result r = rs.next(); r != null; r = rs.next()) {
// r will now have all HBase columns that start with "fluffy",
// which would represent a single row
for (KeyValue kv : r.raw()) {
// each kv represent - the latest version of - a column
	System.out.print("Row Key: " + new String(kv.getRow()) + " ");
	System.out.print("Family: " + new String(kv.getFamily()) + " ");
	System.out.print("Qualifier: " + new String(kv.getQualifier())
	+ " ");
	System.out.println("Value: " + new String(kv.getValue()));
	System.out.print("Timestamp: " + kv.getTimestamp() + " ");
	System.out.println();
}
}
*/
}

public static void getOneRecord(String tableName, String rowKey)
throws IOException {

HTable table = new HTable(conf, tableName);
Get get = new Get(rowKey.getBytes());
Result rs = table.get(get);

for (KeyValue kv : rs.raw()) {

System.out.print("Row Key: " + new String(kv.getRow()) + " ");
System.out.print("Family: " + new String(kv.getFamily()) + " ");
System.out.print("Qualifier: " + new String(kv.getQualifier())
+ " ");
System.out.println("Value: " + new String(kv.getValue()));
System.out.print("Timestamp: " + kv.getTimestamp() + " ");
System.out.println();

}
}


}