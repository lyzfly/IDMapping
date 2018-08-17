package Reducer;

import bean.fact_idBean;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Id_typepercentageReducer extends TableReducer<Text, fact_idBean,ImmutableBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<fact_idBean> values, Context context) throws IOException, InterruptedException {

        int imeisum = 0;
        int Macsum = 0;
        int qqsum = 0;
        int Mobilesum = 0;

        for(fact_idBean val : values){
            if(val.getId_type().equals("imei")){
                imeisum++;
            }else if(val.getId_type().equals("Mac")){
                Macsum++;
            }else if(val.getId_type().equals("qq")){
                qqsum++;
            }else if(val.getId_type().equals("mdn")){
                Mobilesum++;
            }
        }
        Put put = new Put(key.getBytes());
        put.addColumn("res".getBytes(), "the percentage of phone ".getBytes(), String.valueOf(imeisum).getBytes());
        put.addColumn("res".getBytes(),"the percentage of pad".getBytes(),String.valueOf(Macsum).getBytes());
        put.addColumn("res".getBytes(),"the percentage of pc".getBytes(),String.valueOf(qqsum).getBytes());
        put.addColumn("res".getBytes(),"the percentage of other".getBytes(),String.valueOf(Mobilesum).getBytes());

        context.write(new ImmutableBytesWritable(key.getBytes()),put);
    }
}
