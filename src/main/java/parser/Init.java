package parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class Init {
    public void initDatabase(String family,String tablename) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTableDescriptor htd = new HTableDescriptor((TableName.valueOf(tablename)));
        HColumnDescriptor cf = new HColumnDescriptor((family));
        htd.addFamily(cf);
        HBaseAdmin hadmin = new HBaseAdmin(conf);
        if(hadmin.tableExists(tablename)){
            if(!hadmin.isTableDisabled(tablename)){
                hadmin.disableTable(tablename);
            }
            hadmin.deleteTable(tablename);
        }
        hadmin.createTable(htd);
        hadmin.close();
    }
}
