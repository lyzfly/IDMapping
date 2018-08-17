package Mapper;

import bean.fact_idBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import parser.bean.TableParser;

import java.io.IOException;

public class ADCountMapper extends Mapper<LongWritable,Text,Text,Text> {

        Text k = new Text();
        fact_idBean v = new fact_idBean();

        public void map(Text key, Text value, Mapper.Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();
            v = TableParser.getfact_id(line);
            /**
             * 按照天取的key
             */
            k.set(v.getDate_id().substring(0,8));
            context.write(k, v);
        }
}
