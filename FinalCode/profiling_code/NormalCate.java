import java.io.IOException;
import java.util.List;
import java.util.Arrays;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

class NormalCateMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final List<String> cates = Arrays.asList("restaurants", "shopping", "home services",  "financial services", "professional services", "hotels & travel", "active life", "health & medical", "arts & entertainment", "pets", "public services & government", "event planning & services", "education", "religious organizations", "automotive",  "local services", "beauty & spas", "real estate", "nightlife", "mass media", "local flavor", "food");
    private static final List<String> cuisines = Arrays.asList("afghan","african","american (new)","american (traditional)","canadian (new)","canadian (traditional)","arabian","argentine","armenian","asian fusion","australian","austrian","bangladeshi","barbeque","basque","belgian","brasseries","brazilian","breakfast & brunch","british","buffets","burgers","burmese","cafes","cafeteria","cajun/creole","cambodian","caribbean","catalan","cheesesteaks","chicken shop","chicken wings","chinese","comfort food","creperies","cuban","czech","delis","diners","dinner theater","ethiopian","fast food","filipino","fish & chips","fondue","food court","food stands","french","game meat","gastropubs","german","gluten-free","greek","guamanian","halal","hawaiian","himalayan/nepalese","honduran","hong kong style cafe","hot dogs","hot pot","hungarian","iberian","indian","indonesian","irish","italian","japanese","kebab","korean","kosher","laotian","latin american","live/raw food","malaysian","mediterranean","mexican","middle eastern","modern european","mongolian","moroccan","new mexican cuisine","nicaraguan","noodles","pakistani","pan asian","persian/iranian","peruvian","pizza","polish","polynesian","pop-up restaurants","portuguese","poutineries","russian","salad","sandwiches","scandinavian","scottish","seafood","singaporean","slovakian","soul food","soup","southern","spanish","sri lankan","steakhouses","supper clubs","sushi bars","syrian","taiwanese","tapas bars","tapas/small plates","tex-mex","thai","turkish","ukrainian","uzbek","vegan","vegetarian","vietnamese","waffles","wraps");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String line = value.toString();
        String[] cols = line.split("\t");
        String cate = cols[32].trim();
        String[] fields = cate.split(",");
      //if(cate.indexOf("restaurants") == 0) {
      //    for(String field : fields)
      //        context.write(new Text(field.trim()), new IntWritable(1));
      //} 
        String my_cate = "";
        int count = 0;
        String my_cuisine = "";
        int ccount = 0;
        for(String field : fields){
            if(cates.contains(field.trim())){
                field = field.trim();
                my_cate += field+"|";
                count += 1;
                if(field.equals("restaurants")){
                    for(String cui : fields){
                        if(cuisines.contains(cui.trim())){
                            my_cuisine += cui.trim()+'|';
                            ccount += 1;
                        }
                    }
                }
            }
        }
        if(my_cate.length() > 0) my_cate = my_cate.substring(0, my_cate.length()-1);
        if(my_cuisine.length() > 0) my_cuisine = my_cuisine.substring(0, my_cuisine.length()-1);
        context.write(new Text("KEY"), new Text(line+my_cate+'\t'+count+'\t'+my_cuisine+'\t'+ccount));
    }
}

class NormalCateReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
        for(Text value: values) {
            context.write(value, new Text(""));
        }
    }
}

public class NormalCate {
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(NormalCate.class);
        job.setJobName("NormalCate");
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(NormalCateMapper.class);
        job.setReducerClass(NormalCateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
