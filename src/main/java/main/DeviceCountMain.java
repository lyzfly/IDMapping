package main;

import Mapper.DeviceCountMapper;
import Reducer.DeviceCountReducer;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 统计每个小时在线设备数量
 * @author LYZ
 */
public class DeviceCountMain extends AbstractMain{

    public static final Logger logger = Logger.getLogger(DeviceCountMain.class);

    public DeviceCountMain(String htablename, String family, Class<? extends AbstractMain> main,
                       Class <? extends Mapper> mapper, Class<? extends TableReducer> reducer){
        super(htablename,family,main,mapper,reducer);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DeviceCountMain("DeviceCount","res",DeviceCountMain.class,DeviceCountMapper.class,DeviceCountReducer.class),args);
    }

}
