package Reducer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class GraphReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable val : values){
            sum +=val.get();
        }
        Put put = new Put(key.getBytes());
        put.addColumn("res".getBytes(),"the amount of mdnpercentage".getBytes(),String.valueOf(sum).getBytes());
    }
}
