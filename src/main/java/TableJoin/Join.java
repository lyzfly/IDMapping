package TableJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * fact_id 和 muid_id 两表根据 ad 进行等值join操作
 * @author LYZ
 */
class JoinMapper extends Mapper<LongWritable,Text,Text,Text>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit  = (FileSplit)context.getInputSplit();
        Path p  = fileSplit.getPath();
        String filename = p.getName();
        String line = value.toString();
        if(filename.contains("fact_id")){
            /** 去空  */
            if(line==null||line.equals("")){
                return;
            }
            /** 去除有缺失值的数据 */
            String arr[] = line.split("\t");
            if(arr.length<15){
                return;
            }
            int n = line.indexOf("\t");
            String outkey = line.substring(0,n);
            String outvalue = line.substring(n+1);
            context.write(new Text(outkey),new Text("a"+outvalue));

        }else if(filename.contains("muid_id")){
            /**去空 */
            if(line==null||line.equals("")){
                return;
            }
            /** 去除有缺失值的数据 */
            String arr[] = line.split("\t");
            if(arr.length<6){
                return;
            }
            String outkey = arr[1];
            String outvalue = arr[0]+"\t"+arr[3];
            context.write(new Text(outkey),new Text("b"+outvalue));

        }
    }
}
class JoinReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List list1 = new ArrayList<String>();
        List list2 = new ArrayList<String>();

        for(Text t : values){
            if(t.toString().startsWith("a")){
                list1.add(t.toString().substring(1));
            }else {
                list2.add(t.toString().substring(1));
            }
        }
        for(int i=0;i<list1.size();i++){
            for(int j=0;j<list2.size();j++){
                context.write(key,new Text(list1.get(i)+"\t"+list2.get(j)));
            }
        }
    }
}
public class Join extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(Join.class);

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"TableJoin");
        job.setJarByClass(Join.class);
        if(args.length<3){
            logger.error("the input is not valid!");
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Join(),args);
    }
}
