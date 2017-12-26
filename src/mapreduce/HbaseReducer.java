package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HbaseReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) 
			throws IOException, InterruptedException {
		IntWritable altMax = new IntWritable(calculAltMax(values));
		context.write(key, altMax);
	}



	private int calculAltMax(Iterable<IntWritable> iterable) {
		int result = -1;
		for (IntWritable alt : iterable) {
			if(alt.get() > result) result = alt.get();
		}
		
		return result;
	}
	
}
