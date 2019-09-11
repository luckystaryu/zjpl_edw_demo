package hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import javax.imageio.IIOException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutDemo extends BaseDemo {
    public static void main(String[] args) throws IOException {
        Table client =hBaseClient.getTable(tableName);
        singleput(client);
        batchput(client);
        client.close();
    }

    /**
     * 单条插入
     * @param client
     * @throws IOException
     */
    private static void singleput(Table client) throws IOException {
        Put put = new Put("r".getBytes());
        put.addColumn("info".getBytes(),"c1".getBytes(),"v1".getBytes());
        put.addColumn("info".getBytes(),"c2".getBytes(),"v2".getBytes());
        client.put(put);
        System.out.println("put r1 to "+client.getName().toString());
    }

    /**
     * 批量插入
     * @param client
     * @throws IOException
     */
    private static void batchput(Table client) throws IOException {
        Put put = new Put("r2".getBytes());
        put.addColumn("info".getBytes(),"c1".getBytes(),"v1".getBytes());
        put.addColumn("info".getBytes(),"c2".getBytes(),"v2".getBytes());

        Put put2 = new Put("r3".getBytes());
        put2.addColumn("info".getBytes(),"c3".getBytes(),"v1".getBytes());
        List<Put> putList= new ArrayList<>();
        putList.add(put);
        putList.add(put2);
        client.put(putList);
        System.out.println("put r2,r3 to"+client.getName().toString());
    }
}
