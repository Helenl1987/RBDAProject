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




class TrendSplitingMapper extends Mapper<LongWritable, Text, Text, Text> {	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		List<Double> list = new ArrayList<Double>();
		int cnt_timewindow = 0;
		double rating = 0.0;
		String res = "";
		String tmpres = "";
		int offset = 0;
		String linetext = value.toString();
		String[] splitstring = linetext.split("\t");
		res = "";
		for(int i = 0; i < 14; ++i) {
			res += String.format("%s\t", splitstring[i]);
		}
		if(Integer.parseInt(splitstring[13]) == 1) {
			list.clear();
			if(splitstring.length != 59+14) {
				context.write(new Text(String.format("record length not equal to 59+14, failed with input:\n %s", linetext)), new Text(""));
			}
			for(int i = 72; i >= 14; --i) {
				rating = Double.parseDouble(splitstring[i]);
				if(rating != -1) {
					if(list.isEmpty()) {
						res += String.format("%s\t", transToDate(72-i));
						offset = 72-i;
					}
					list.add(rating);
				}
			}
			cnt_timewindow = 0;
			tmpres = "";
			int l = 0, r = 0;
			while(l < list.size()) {
				r = l+1;
				while(r < list.size() && list.get(r).equals(list.get(l))) {
					r++;
				}
				tmpres += String.format("%s\t%s\t%s\t", transToDate(l+offset), transToDate(r-1+offset), String.valueOf(list.get(l)));
				if(r < list.size()) {
					tmpres += String.format("%s\t", String.valueOf(list.get(r)-list.get(l)));
				}
				else {
					tmpres += String.format("%s\t", String.valueOf(-0.0));
				}
				cnt_timewindow++;
				l = r;
			}
			res += String.format("%s\t", String.valueOf(cnt_timewindow));
			res += tmpres;
			context.write(new Text(res), new Text(""));
		}
		else {
		}
	}

	public String transToDate(int idx) {
		String res = "";
		int year = 2014 + (idx / 12);
		int month = idx % 12;
		res = String.format("%04d_%02d", year, month);
		return res;
	}
}





class TrendSplitingReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(key, new Text(""));
	}
}





public class TrendSpliting {
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(TrendSpliting.class);
		job.setJobName("Trend.Spliting");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(TrendSplitingMapper.class);
		job.setReducerClass(TrendSplitingReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}









