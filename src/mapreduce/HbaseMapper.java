package mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HbaseMapper extends TableMapper<Text, IntWritable>{
	
	private final byte[] FAMILY = Bytes.toBytes("commune");
	private final byte[] NAME = Bytes.toBytes("Nom	");
	private final byte[] ALTITUDE = Bytes.toBytes("Altitude	");

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context) 
			throws IOException, InterruptedException {
		byte[] name = value.getValue(FAMILY, NAME);
		byte[] alti = value.getValue(FAMILY, ALTITUDE);
		if(name != null && alti != null) {
			String n = Bytes.toString(name);
			int a = Bytes.toInt(alti);
			context.write(new Text(n), new IntWritable(a));
		}
 		
	}
	
	
	
	
}



