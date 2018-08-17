package main;

import InitHtable.Init;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

import java.io.IOException;

/**
 * 抽象Job启动类
 * @author LYZ
 */
public abstract class AbstractMain extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(AbstractMain.class);

    private String  htable_famiily;
    private String tablename;
    private Class<? extends AbstractMain> main;
    private Class<? extends Mapper> mapper;
    private Class<? extends TableReducer> reducer;

    public AbstractMain(String htable_famiily,String tablename,Class<? extends AbstractMain> main,
                        Class<? extends  Mapper> mapper,Class<? extends TableReducer> reducer){

        this.htable_famiily = htable_famiily;
        this.tablename = tablename;
        this.main = main;
        this.mapper = mapper;
        this.reducer = reducer;
    }

    public String hello(){
        return "sdas";
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length<2){
            logger.error("Usage: wordcount <in> [<in>...] <out>");
            System.exit(-1);
        }

        Init init = new Init();
        init.initDatabase(htable_famiily,tablename);
        Configuration conf = new Configuration();
        Path inputPath  = new Path(args[0]);
        conf.set(TableOutputFormat.OUTPUT_TABLE,args[1]);
        Job job = Job.getInstance(conf,tablename);
        job.setJarByClass(main);
        FileInputFormat.setInputPaths(job, inputPath);
        job.setMapperClass(mapper);
        TableMapReduceUtil.initTableReducerJob(tablename, reducer, job);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setNumReduceTasks(10);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
