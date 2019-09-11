package hbase;

import org.apache.hadoop.hbase.TableName;

public abstract class BaseDemo {
    private static String zkquorum="172.16.1.61:2181,172.16.1.62:2181,172.16.1.63:2181";
    protected static HBaseClient hBaseClient= new HBaseClient(zkquorum);
    protected static final String NS="ZJPL";
    protected static final String TBL="test_hbase";
    protected static final TableName tableName=TableName.valueOf(NS,TBL);
}
