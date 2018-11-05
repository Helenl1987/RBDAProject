import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataProfileReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String dataType = conf.get("data.type");
		if(dataType.equals("business")) {
			profile_business(key, values, context);
		}
		else if(dataType.equals("user")) {
			profile_user(key, values, context);
		}
	}

	public void profile_business(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String res = "";
		for(Text value: values) {
			res += value.toString();
		}
		context.write(new Text(res), new Text(""));
	}

	public void profile_user(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
	}
}






