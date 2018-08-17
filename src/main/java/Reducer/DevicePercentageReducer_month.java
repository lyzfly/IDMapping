package Reducer;

import bean.fact_idBean;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class DevicePercentageReducer_month extends TableReducer<Text, fact_idBean,ImmutableBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<fact_idBean> values, Context context) throws IOException, InterruptedException {

        int phonesum = 0;
        int padsum = 0;
        int pcsum = 0;
        int othersum = 0;

        float phonepercentage = 0;
        float padpercentage = 0;
        float pcpercentage = 0;
        float otherpercentage = 0;

        for(fact_idBean val : values){
            if(val.getDevice().contains("Phone")||(val.getOs().contains("Android")&&!val.getOs().contains("Pad"))){
                phonesum++;
            }else if(val.getDevice().contains("Pad")){
                padsum++;
            }else if(val.getOs().contains("PC")){
                pcsum++;
            }else{
                othersum++;
            }
            int allsum = phonesum+padsum+pcsum+othersum;
            phonepercentage = phonesum*100/allsum;
            padpercentage = padsum*100/allsum;
            pcpercentage = pcsum*100/allsum;
            otherpercentage = othersum*100/othersum;

            DecimalFormat df = new DecimalFormat("#.00");
            phonepercentage = Float.valueOf(df.format(phonepercentage));
            padpercentage = Float.valueOf(df.format(padpercentage));
            pcpercentage = Float.valueOf(df.format(pcpercentage));
            otherpercentage = Float.valueOf(df.format(otherpercentage));
        }
        Put put = new Put(key.getBytes());
        put.addColumn("res".getBytes(), "the percentage of phone ".getBytes(), String.valueOf(phonepercentage).getBytes());
        put.addColumn("res".getBytes(),"the percentage of pad".getBytes(),String.valueOf(padpercentage).getBytes());
        put.addColumn("res".getBytes(),"the percentage of pc".getBytes(),String.valueOf(pcpercentage).getBytes());
        put.addColumn("res".getBytes(),"the percentage of other".getBytes(),String.valueOf(otherpercentage).getBytes());

        context.write(new ImmutableBytesWritable(key.getBytes()),put);
    }
}
