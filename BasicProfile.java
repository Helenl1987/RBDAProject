import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.ArrayList;

//if field is numeric("plain" int or double), calculate MAX/MIN/AVG
//if field is string, calculate MAX_LEN/MIN_LEN/AVG_LEN
//output in single file with a single line
//personally thought there id no benefit for MR to do this...
class BasicProfileMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Pattern dpattern = Pattern.compile("(0|([1-9]+[0-9]*))\\.[0-9]+");
        Pattern ipattern = Pattern.compile("0|([1-9]+[0-9]*)");
        String line = value.toString();
        String[] fields = line.split("\t");
        String result = "";
        for(int i = 0; i < fields.length; i++){
            Matcher isInt = ipattern.matcher(fields[i]);
            Matcher isDouble = dpattern.matcher(fields[i]);
            if(isInt.matches() || isDouble.matches()){
                result += (fields[i]+"\t");
            }
            else{
               String[] words = fields[i].split(" ");
               if(words.length == 1) result += (fields[i].length() + "\t");
               else result += (words.length + "\t");
            }
        }
        context.write(new Text("one"), new Text(result));
    }
}

class BasicProfileReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean first = true;
        ArrayList<Double> calculate = new ArrayList<Double>();
        int count = 0;
        for(Text value: values) {
            count += 1;
            String[] parts = value.toString().split("\t");
            if(first){
                for(int i = 0; i < parts.length - 1; i++){
                    calculate.add(Double.valueOf(parts[i].toString()));
                    calculate.add(Double.valueOf(parts[i].toString()));
                    calculate.add(Double.valueOf(parts[i].toString()));
                }
                first = false;
            }
            for(int i = 0; i < parts.length - 1; i++){
                Double temp = Double.valueOf(parts[i].toString());
                if(temp > calculate.get(3*i)) calculate.set(3*i, temp);
                if(temp < calculate.get(3*i)) calculate.set(3*i+1, temp);
                Double current_sum = calculate.get(3*i+2);
                calculate.set(3*i+2, temp+current_sum);
            }
        }
        //concat the calculation
        String result = "";
        for(int i = 0; i < (calculate.size()/3); i++){
            result += (calculate.get(3*i) + "|" + calculate.get(3*i+1) + "|"+ (calculate.get(3*i+2)/count) + ",");
        }
        context.write(new Text("Basic_Profile"), new Text(result));
    }
}

public class BasicProfile {
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(BasicProfile.class);
        job.setJobName("BasicProfile");
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(BasicProfileMapper.class);
        job.setReducerClass(BasicProfileReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

