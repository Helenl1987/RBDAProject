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




class DataProfileMapper extends Mapper<LongWritable, Text, Text, Text> {
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
			// clean_business(key, value, context);
			profile_business(key, value, context);
		}
		else if(dataType.equals("user")) {
			// clean_user(key, value, context);
			profile_user(key, value, context);
		}
	}

	public void profile_business(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splitstring = line.split("\t");
		for(int i = 0; i < splitstring.length; ++i) {
			if(splitstring[i].equals("NULL"))
				continue;
			if(schema_business[i] == 0) {
				context.write(new Text(String.valueOf(i+1)), new Text(String.valueOf(splitstring[i].length())));
			}
			else if(schema_business[i] == 1) {
				try {
					int num = Integer.parseInt(splitstring[i]);
					context.write(new Text(String.valueOf(i+1)), new Text(splitstring[i]));
				} catch(NumberFormatException e) {
					context.write(new Text(String.valueOf(i+1)), new Text(String.format("(%s:%s)", splitstring[34], splitstring[i])));
				}
			}
			else {
				try {
					double num = Double.parseDouble(splitstring[i]);
					context.write(new Text(String.valueOf(i+1)), new Text(splitstring[i]));
				} catch(NumberFormatException e) {
					context.write(new Text(String.valueOf(i+1)), new Text(String.format("(%s:%s)", splitstring[34], splitstring[i])));
				}
			}
		}
	}


	public void profile_user(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splitstring = line.split("\t");
		for(int i = 0; i < splitstring.length; ++i) {
			if(splitstring[i].equals("NULL"))
				continue;
			if(schema_user[i] == 0) {
				context.write(new Text(String.valueOf(i+1)), new Text(String.valueOf(splitstring[i].length())));
			}
			else if(schema_user[i] == 1) {
				try {
					int num = Integer.parseInt(splitstring[i]);
					context.write(new Text(String.valueOf(i+1)), new Text(splitstring[i]));
				} catch(NumberFormatException e) {
					context.write(new Text(String.valueOf(i+1)), new Text(String.format("(%s:%s)", splitstring[12], splitstring[i])));
				}
			}
			else {
				try {
					double num = Double.parseDouble(splitstring[i]);
					context.write(new Text(String.valueOf(i+1)), new Text(splitstring[i]));
				} catch(NumberFormatException e) {
					context.write(new Text(String.valueOf(i+1)), new Text(String.format("(%s:%s)", splitstring[12], splitstring[i])));
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





class DataProfileReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String dataType = conf.get("data.type");
		if(dataType.equals("business")) {
			// clean_business(key, values, context);
			profile_business(key, values, context);
		}
		else if(dataType.equals("user")) {
			// clean_user(key, values, context);
			profile_user(key, values, context);
		}
	}

	public void profile_business(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double maxi = 0.0, mini = 0.0, tmp = 0.0;
		String res = "";
		for(Text value: values) {
			try {
				tmp = Double.parseDouble(value.toString());
				if(tmp > maxi)
					maxi = tmp;
				if(tmp < mini)
					mini = tmp;
			} catch(NumberFormatException e) {
				res += value.toString();
			}
		}
		String maxistr = "", ministr = "";
		if(maxi % 1.0 == 0)
			maxistr = String.valueOf((long)maxi);
		else
			maxistr = String.valueOf(maxi);
		if(mini % 1.0 == 0)
			ministr = String.valueOf((long)mini);
		else
			ministr = String.valueOf(mini);
		res = String.format("(%s, %s)", maxistr, ministr) + res;
		context.write(key, new Text(res));
	}

	public void profile_user(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		profile_business(key, values, context);
	}
}







public class DataProfile {
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
		job.setJarByClass(DataProfile.class);
		job.setJobName("DataProfile");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(DataProfileMapper.class);
		job.setReducerClass(DataProfileReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}









