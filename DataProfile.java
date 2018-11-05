import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import java.util.*;

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
