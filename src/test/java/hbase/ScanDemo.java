package hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

public class ScanDemo extends BaseDemo{
    public static void main(String[] args) throws IOException {
        Table client = hBaseClient.getTable(tableName);
        Scan scan = new Scan();
        //scan.setStartRow("r".getBytes());
        //scan.setStopRow("r4".getBytes());

        //使用过滤器来缩小范围：
        //1.RowFilter
        //Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("r")));
        //scan.setFilter(rowFilter);
        //2.PrefixFilter
        //Filter prefixFilter = new PrefixFilter(Bytes.toBytes("r"));
        //scan.setFilter(prefixFilter);
        //3.KeyOnlyFilter
//        Filter keyOnlyFilter = new KeyOnlyFilter();
//        scan.setFilter(keyOnlyFilter);
        //4.RandomRowFilter
        Filter randomRowFilter = new RandomRowFilter((float)0.5);
        scan.setFilter(randomRowFilter);
        //5.ColumnPrefixFilter 列名前缀
        //6.ValueFilter 值过滤
        //7.ColumnCountGetFilter 一行最多返回多少列
        //8.SingleColumnValueFilter 用一列的值决定这一行的数据是否被过滤
        //9.SingleColumnValueExcludeFilter
        //10.SkipFilter 附加过滤器，其与ValueFilter结合使用
        //11.WhileMatchFilter 如果你想要在遇到某种条件数据之前的数据时，就可以使用这个过滤器；当遇到不符合设定条件的数据的时候，整个扫描也就结束了
        //12.FilterList 用于综合使用多个过滤器
        ResultScanner resultScanner =client.getScanner(scan);
        for(Result result:resultScanner){
            NavigableMap<byte[],byte[]> cf= result.getFamilyMap("info".getBytes());
            for(Map.Entry<byte[],byte[]> e:cf.entrySet()){
                System.out.println("row:"+new String(result.getRow()) +","
                +"cf:info"+",col:"+new String(e.getKey())+","
                +"val:"+new String(e.getValue()));
            }
        }
    }
}
