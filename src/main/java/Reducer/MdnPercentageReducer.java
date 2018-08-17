package Reducer;

import bean.fact_idBean;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import util.MdnCheck;

import java.io.IOException;
import java.text.DecimalFormat;

public class MdnPercentageReducer extends TableReducer<Text,fact_idBean,ImmutableBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<fact_idBean> values, Context context) throws IOException, InterruptedException {

        int DianXinsum = 0;
        int LianTongsum = 0;
        int Yidongsum = 0;
        int sum = 0;

        float DianXinpercentage = 0;
        float LianTongPercentage = 0;
        float YiDongpercentage = 0;

        for(fact_idBean val : values){
            if(MdnCheck.isDianXin(val.getID())){
                DianXinsum++;
            }else if(MdnCheck.isLianTong(val.getID())){
                LianTongsum++;
            }else if(MdnCheck.isYiDong(val.getID())){
                Yidongsum++;
            }
        }
        sum = DianXinsum+LianTongsum+Yidongsum;
        DecimalFormat df = new DecimalFormat("#.00");
        DianXinpercentage = DianXinsum*100/sum;
        DianXinpercentage = Float.valueOf(df.format(DianXinpercentage));
        LianTongPercentage = Float.valueOf(df.format(LianTongPercentage));
        YiDongpercentage = Float.valueOf(df.format(YiDongpercentage));

        Put put = new Put(key.getBytes());
        put.addColumn("res".getBytes(), "the percentage of DianXin ".getBytes(), String.valueOf(DianXinpercentage).getBytes());
        put.addColumn("res".getBytes(),"the percentage of LianTong".getBytes(),String.valueOf(LianTongPercentage).getBytes());
        put.addColumn("res".getBytes(),"the percentage of YiDong".getBytes(),String.valueOf(YiDongpercentage).getBytes());

        context.write(new ImmutableBytesWritable(key.getBytes()),put);
    }
}
