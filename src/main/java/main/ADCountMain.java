package main;

import Mapper.ADCountMapper;
import Reducer.ADCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.log4j.Logger;


/**
 * 统计每个小时内的在线家庭数量
 * @author LYZ
 */
public class ADCountMain extends AbstractMain {

    private static final Logger logger = Logger.getLogger(ADCountMain.class);

    public ADCountMain(String htablename, String family, Class<? extends AbstractMain> main,
                       Class <? extends Mapper> mapper, Class<? extends TableReducer> reducer){
        super(htablename,family,main,mapper,reducer);
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(conf,new ADCountMain("ADCount","res",ADCountMain.class,ADCountMapper.class,ADCountReducer.class),args);

    }


}
