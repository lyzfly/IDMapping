package main;

import bean.fact_idBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import parser.bean.TableParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 统计有多人同时拥有qq和手机
 * @author LYZ
 */
public class QQandMDN{

    public static class EachMapper extends Mapper<LongWritable,Text,Text,fact_idBean> {

        Text k = new Text();
        fact_idBean v = new fact_idBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                String line = value.toString();
                v = TableParser.getfact_id(line);
                k = new Text(v.getMuid());
                context.write(k,v);

        }
    }

    public static class EachReducer extends Reducer<Text,fact_idBean,Text,IntWritable>{
        /**
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<fact_idBean> values, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList();
            for(fact_idBean bean : values){
                if(!list.contains("qq")&&bean.getId_type().equals("qq")){
                    list.add("qq");
                }
                if(!list.contains("mdn")&&bean.getId_type().equals("mdn")){
                    list.add("mdn");
                }
                if(list.contains("qq")&&list.contains("mdn")){

                    break;
                }
            }
            context.write(new Text("one"),new IntWritable(1));
        }
    }

    public static class SumMapper extends Mapper<Text,Integer,Text,Integer>{
        @Override
        protected void map(Text key, Integer value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            context.write(key,value);

        }
    }
    public static class SumReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
           for(IntWritable val : values){
               sum += val.get();
           }
            Put put = new Put(key.getBytes());
            put.addColumn("res".getBytes(), "the amout of qqandmdn".getBytes(), String.valueOf(sum).getBytes());
            context.write(new ImmutableBytesWritable(key.getBytes()),put);
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job1 = new Job(conf,"each");
        job1.setJarByClass(QQandMDN.class);
        job1.setMapperClass(EachMapper.class);
        job1.setReducerClass(EachReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(fact_idBean.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job1);
        FileInputFormat.addInputPath(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]));
        Job job2 = new Job(conf,"sum");
        job2.setJarByClass(QQandMDN.class);
        job2.setMapperClass(SumMapper.class);
        job2.setReducerClass(SumReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(ImmutableBytesWritable.class);
        job2.setOutputValueClass(Put.class);
        ControlledJob controlledJob2 = new ControlledJob(conf);
        controlledJob2.setJob(job2);
        controlledJob2.addDependingJob(controlledJob1);
        FileInputFormat.addInputPath(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
        JobControl jobControl = new JobControl("qqadnmdn");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        Thread t = new Thread(jobControl);
        t.start();

        while (true) {

            if(jobControl.allFinished()){
                System.out.println(jobControl.getSuccessfulJobList());
                jobControl.stop();
                break;
            }
        }
    }

}
