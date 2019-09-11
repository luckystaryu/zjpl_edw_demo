package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * 通过zk连接hbase,创建HBaseClient
 */
public class HBaseClient {
    private Connection conn;

    /**
     * 通过zk集群，创建hbase连接
     * @param zkQuorum
     */
    public HBaseClient(String zkQuorum){
        Configuration conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",zkQuorum);
        try {
            conn= ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 负责DDL
     */
    public Admin getAdmin(){
        try {
            return conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void createTable(String tableName) throws IOException {
        HBaseAdmin admin =(HBaseAdmin)getAdmin();
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(String.valueOf(tableName)));
        HColumnDescriptor htd_info =new HColumnDescriptor("info");
        htd.addFamily(htd_info);
        htd_info.setMaxVersions(3);
        admin.createTable(htd);
        admin.close();

    }
    /**
     * 负责DML
     */
    public Table getTable(TableName tableName){
        try {
            return conn.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * upsert
     */
    public void upsert(TableName tableName,HbaseRecord record){
        Put put= new Put(record.getRowkey().getBytes(),record.getVersion());
        record.getData().forEach((k,v)->put.addColumn("info".getBytes(),k.getBytes(),v.getBytes()));
        try (Table table = getTable(tableName)) {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
