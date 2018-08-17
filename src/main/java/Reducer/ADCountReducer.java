package Reducer;


import bean.fact_idBean;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 统计每个小时家庭数量
 */
public class ADCountReducer extends TableReducer<Text, fact_idBean,ImmutableBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<fact_idBean> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        List<String> list = new ArrayList();
        for(fact_idBean val : values){
            if(!list.contains(val.getInner_id())){
                list.add(val.getInner_id());
                count++;
            }
        }
        Put put = new Put(key.getBytes());
        put.addColumn("res".getBytes(), "the amount of AD".getBytes(), String.valueOf(count).getBytes());
        context.write(new ImmutableBytesWritable(key.getBytes()),put);
    }
}
