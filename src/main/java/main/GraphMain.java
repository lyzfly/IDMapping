package main;

import Reducer.GraphReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import Mapper.GraphMapper;

/**
 * 统计每种电信手机号占比对应的家庭总数
 * @author LYZ r
 */

public class GraphMain extends  AbstractMain {


    public GraphMain(String htable_famiily, String tablename, Class<? extends AbstractMain> main, Class<? extends Mapper> mapper, Class<? extends TableReducer> reducer) {
        super(htable_famiily, tablename, main, mapper, reducer);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        ToolRunner.run(conf,new GraphMain("res","graph",GraphMain.class,GraphMapper.class,GraphReducer.class),args);
    }
}

