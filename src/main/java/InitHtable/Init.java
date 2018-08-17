package InitHtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * 创建hbase一个表
 * @author LYZ
 */
public class Init {
    public static void initDatabase(String family,String tablename) throws IOException {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tablename));
        HColumnDescriptor cf = new HColumnDescriptor(family);
        htd.addFamily(cf);
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);
        if (hAdmin.tableExists(tablename)) {
            if (!hAdmin.isTableDisabled(tablename)) {
                hAdmin.disableTable(tablename);
            }
            hAdmin.deleteTable(tablename);
        }
        hAdmin.createTable(htd);
        hAdmin.close();
    }
}
