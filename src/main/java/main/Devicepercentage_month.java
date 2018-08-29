package main;

import Mapper.DecvicepercentageMapper_month;
import Reducer.DevicePercentageReducer_month;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 统计每个月各个设备所占比，保留小数点后两位有效数字
 * @author LYZ
 */
public class Devicepercentage_month extends AbstractMain{

    private static final Logger logger = Logger.getLogger(ADCountMain.class);

    public Devicepercentage_month(String htablename, String family, Class<? extends AbstractMain> main,
                       Class <? extends Mapper> mapper, Class<? extends TableReducer> reducer){
        super(htablename,family,main,mapper,reducer);
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(conf,new ADCountMain("Devicepercentage","res", Devicepercentage_month.class, DecvicepercentageMapper_month.class, DevicePercentageReducer_month.class),args);

    }

}
