import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataProfileMapper extends Mapper<LongWritable, Text, Text, Text> {
	static final int CATEGORIES = 32;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String dataType = conf.get("data.type");
		if(dataType.equals("business")) {
			profile_business(key, value, context);
		}
		else if(dataType.equals("user")) {
			profile_user(key, value, context);
		}
	}

	public void profile_business(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splitstring = line.split("\t");
		String categories = splitstring[CATEGORIES];
		if(categories.equals("NULL") == false) {
			if(categories.toLowerCase().contains("restaurant")) {
				context.write(new Text(key.toString()), value);
			}
		}
	}

	public void profile_user(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
	}
}











