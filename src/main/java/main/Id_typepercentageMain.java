package main;

import Reducer.ADCountReducer;
import Mapper.Id_typepercentageMapper;
import Reducer.Id_typepercentageReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Id_typepercentageMain extends AbstractMain{

    private static final Logger logger = Logger.getLogger(ADCountMain.class);

    public Id_typepercentageMain(String htablename, String family, Class<? extends AbstractMain> main,
                       Class <? extends Mapper> mapper, Class<? extends TableReducer> reducer){
        super(htablename,family,main,mapper,reducer);
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(conf,new ADCountMain("ID_type","res",Id_typepercentageMain.class, Id_typepercentageMapper.class, Id_typepercentageReducer.class),args);
    }
}
