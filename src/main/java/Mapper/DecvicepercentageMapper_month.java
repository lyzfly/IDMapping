package Mapper;

import bean.fact_idBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import parser.bean.TableParser;

import java.io.IOException;

public class DecvicepercentageMapper_month extends Mapper<LongWritable,Text,Text,fact_idBean> {


    Text k = new Text();
    fact_idBean v = new fact_idBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        k.set(v.getDate_id());
        v = TableParser.getfact_id(line);
        context.write(k,v);
    }
}
