package Mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;


import java.io.IOException;

public class GraphMapper extends TableMapper<Text,IntWritable> {

    private static IntWritable One = new IntWritable(1);
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        String mapkey = new String(value.getValue(Bytes.toBytes("res"),Bytes.toBytes("the percentage of Dianxin")));
        context.write(new Text(mapkey),One);

    }

}
