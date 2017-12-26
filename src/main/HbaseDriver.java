package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapreduce.HbaseMapper;
import mapreduce.HbaseReducer;

public class HbaseDriver {
	
	public static void main(String[] args) throws Exception{
		
		// Validation si les deux arguments ont été pris en ligne de commande
		if (args.length != 1) {
			System.err.println("Usage: HbaseDriver <output_directory>");
			System.exit(-1);
		}
		
		// Instanciation d'un job
		Configuration hbaseConf = HBaseConfiguration.create();
		Job job = Job.getInstance(hbaseConf);
		
		
		// Specification des jars
		job.setJarByClass(HbaseDriver.class);
		job.setOutputKeyClass(Text.class);
	 	job.setOutputValueClass(IntWritable.class);
	 	FileOutputFormat.setOutputPath(job, new Path(args[0]));
	 	TableMapReduceUtil.initTableMapperJob("mesure", // input table
 												new Scan(), // input scan instance
												HbaseMapper.class, // mapper class
												Text.class, // type of output key
												IntWritable.class, // type of output value
												job);
		 job.setReducerClass(HbaseReducer.class);
		 TableMapReduceUtil.addDependencyJars(job);
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
