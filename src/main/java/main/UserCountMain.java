package main;

import Mapper.UserCountMapper;
import Reducer.UserCountReducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 统计每个小时在线的用户数量
 * @author LYZ
 */
public class UserCountMain extends AbstractMain{

    public static final Logger logger = Logger.getLogger(UserCountMain.class);

    public UserCountMain(String htablename, String family, Class<? extends AbstractMain> main,
                           Class <? extends Mapper> mapper, Class<? extends TableReducer> reducer){
        super(htablename,family,main,mapper,reducer);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DeviceCountMain("UserCount","res",UserCountMain.class, UserCountMapper.class, UserCountReducer.class),args);
    }
}
