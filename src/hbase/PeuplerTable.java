package hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class PeuplerTable {
		
	public static void main(String[] args) throws IOException {
		Configuration hbaseConf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(hbaseConf);
		Table table = connection.getTable(TableName.valueOf("mesure"));
		
		//File file = new File("mesure.csv");
		BufferedReader CSVFile = new BufferedReader(new FileReader("/home/oussema/Bureau/mesure.csv"));
		CSVFile.readLine();
		for(String line = CSVFile.readLine(); line!=null; line = CSVFile.readLine()){
			String[] ls = line.split(";");
			
			Put put = new Put(Bytes.toBytes(ls[0]));
			put.addColumn(Bytes.toBytes("commune"), Bytes.toBytes("Nom"), Bytes.toBytes(ls[1]));
			put.addColumn(Bytes.toBytes("commune"), Bytes.toBytes("Latitude"), Bytes.toBytes(ls[2]));
			put.addColumn(Bytes.toBytes("commune"), Bytes.toBytes("Longitude"), Bytes.toBytes(ls[3]));
			put.addColumn(Bytes.toBytes("commune"), Bytes.toBytes("Altitude"), Bytes.toBytes(ls[4]));

			table.put(put);
		}
		table.close();
	}
}
