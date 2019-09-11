package hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.compress.Compression;

public class AdminDemo extends BaseDemo {
    public static void main(String[] args) throws Exception{
        Admin admin = hBaseClient.getAdmin();
        //若存在,则清空
        if(admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            admin.deleteNamespace(NS);
            System.out.println("delete table "+tableName.getNameAsString());
        }
        //创建ns
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(NS).build();
        admin.createNamespace(namespaceDescriptor);
        System.out.println("create ns "+NS);
        //创建表
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

        //列簇属性
//        {
//            BLOOMFILTER =>ROW,
//            VERSION =>'1',
//            IN_MEMORY => false,
//            KEEP_DELETED_CELLS =>FALSE,
        //    DATA_BLOCK_ENCODING =>NONE,
        //    TTL => 'FOREVER',
        //    COMPRESSION => 'SNAPPY',
        //    MIN_VERSIONS => '0',
        //    BLOCKCACHE =>'true',
        //    BLOCKSIZE => '65536',
        //    REPLICATION_SCOPE => '0'
//        }
        //列簇1
        HColumnDescriptor cf1 = new HColumnDescriptor("info");
        cf1.setCompressionType(Compression.Algorithm.SNAPPY);
        hTableDescriptor.addFamily(cf1);
        //列簇2
        HColumnDescriptor cf2 = new HColumnDescriptor("info2");
        hTableDescriptor.addFamily(cf2);

        admin.createTable(hTableDescriptor);
        admin.close();
    }
}
