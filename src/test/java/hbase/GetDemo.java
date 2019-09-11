package hbase;


import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetDemo extends BaseDemo{
    public static void main(String[] args) throws IOException {
        Table client= hBaseClient.getTable(tableName);
        singleGet(client);
        batchGet(client);
        client.close();
    }

    private static void batchGet(Table client) throws IOException {
        Get get = new Get("r2".getBytes());
        Get get2 = new Get("r3".getBytes());
        List<Get> getList =new ArrayList<>();
        getList.add(get);
        getList.add(get2);
        Result[] results= client.get(getList);
        for(Result result:results){
            System.out.println(Bytes.toString(result.getRow())+"/info:c2=>"+Bytes.toString(result.getValue("info".getBytes(),"c2".getBytes())));
        }
    }

    private static void singleGet(Table client) throws IOException {
        Get get = new Get("r".getBytes());
        Result result =client.get(get);
        System.out.println("r/info:c1=>"+ Bytes.toString(result.getValue("info".getBytes(),"c1".getBytes())));
    }
}
