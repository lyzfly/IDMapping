package main;

import Mapper.MdnPercentageMapper;
import Reducer.MdnPercentageReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 统计移动手机号，联通手机号，电信手机号所占比
 * @author LYZ
 */
public class MdnpercentageMain extends AbstractMain {
    private static final Logger logger = Logger.getLogger(ADCountMain.class);

    public MdnpercentageMain(String htablename, String family, Class<? extends AbstractMain> main,
                             Class <? extends Mapper> mapper, Class<? extends TableReducer> reducer){
        super(htablename,family,main,mapper,reducer);
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(conf,new ADCountMain("mdnpercentage","res", MdnpercentageMain.class, MdnPercentageMapper.class, MdnPercentageReducer.class),args);
    }
}
