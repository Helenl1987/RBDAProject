import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;

import java.util.*;
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




class DataCleaningMapper extends Mapper<LongWritable, Text, Text, Text> {
	// business
	static final int CATEGORIES = 32;
	// column numbers after pre-counting (total restaurant 5791, drop < 10000)
	// dropping: 42, 43, 60, 62-103
	// remaining: 1-41, 44-59, 61
	static final int[] schema_business = {
		0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 
		1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 
		0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 
		0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
		0, 0, 0, 0, 0, 0, 0, 0};
	// string->0, int->1, double->2

	//user
	static final int REVIEW_COUNT = 5;
	static final int[] schema_user = {
		1, 1, 1, 2, 1, 1, 1, 0, 1, 1, 
		0, 1, 0, 0, 1, 0, 1, 1, 1, 1, 
		1, 1};

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String dataType = conf.get("data.type");
		if(dataType.equals("business")) {
			clean_business(key, value, context);
			// profile_business(key, value, context);
		}
		else if(dataType.equals("user")) {
			clean_user(key, value, context);
			// profile_user(key, value, context);
		}
	}

	public void clean_business(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splitstring = line.split("\t");
		String categories = splitstring[CATEGORIES];
		String res = "";
		if(categories.equals("NULL") == false) {
			if(categories.toLowerCase().contains("restaurant")) {
				for(int i = 0; i < splitstring.length; ++i) {
					// if(splitstring[i].equals("NULL") == false) {
					// 	context.write(new Text(String.valueOf(i+1)), new Text("1"));
					// }
					if(i < 61 && i != 41 && i != 42 && i != 59) {
						res += String.format("%s\t", splitstring[i]);
					}
				}
				context.write(new Text(res), new Text(""));
			}
		}
	}

	public void clean_user(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splitstring = line.split("\t");
		int num = Integer.parseInt(splitstring[REVIEW_COUNT]);
		if(num > 0) {
			for(int i = 0; i < splitstring.length; ++i) {
				if(splitstring[i].trim().equals("0") == false) {
					context.write(new Text(String.valueOf(i+1)), new Text("1"));
				}
			}
		}
	}

	// public boolean isInteger(String str) {
	// 	Pattern pattern = Pattern.compile("[0-9]+");
	// 	Matcher flag = pattern.matcher(str);
	// 	if(!flag.matches())
	// 		return false;
	// 	else
	// 		return true;
	// }

	// public boolean isDouble(String str) {
	// 	Pattern pattern = Pattern.compile("[0-9]+\\.[0-9]+");
	// 	Matcher flag = pattern.matcher(str);
	// 	if(!flag.matches())
	// 		return false;
	// 	else
	// 		return true;
	// }
}





class DataCleaningReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String dataType = conf.get("data.type");
		if(dataType.equals("business")) {
			clean_business(key, values, context);
			// profile_business(key, values, context);
		}
		else if(dataType.equals("user")) {
			clean_user(key, values, context);
			// profile_user(key, values, context);
		}
	}

	public void clean_business(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// int res = 0;
		// for(Text value: values) {
		// 	res += 1;
		// }
		context.write(key, new Text(""));
	}

	public void clean_user(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int res = 0;
		for(Text value: values) {
			res += 1;
		}
		context.write(key, new Text(String.valueOf(res)));
	}
}







public class DataCleaning {
	public static void main(String[] args) throws Exception {
		String[] splitstring = args[0].split("/");
		String dataType = splitstring[splitstring.length-1];
		if(dataType.contains("business")) {
			dataType = "business";
		}
		else if(dataType.contains("user")) {
			dataType = "user";
		}
		
		Configuration conf = new Configuration();
		conf.set("data.type", dataType);

		Job job = Job.getInstance(conf);
		job.setJarByClass(DataCleaning.class);
		job.setJobName("DataCleaning");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(DataCleaningMapper.class);
		job.setReducerClass(DataCleaningReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}









