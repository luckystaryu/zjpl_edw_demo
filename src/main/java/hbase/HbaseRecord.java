package hbase;

import java.util.HashMap;
import java.util.Map;

public class HbaseRecord {
    private String rowkey;
    private Long   version = System.currentTimeMillis();
    private Map<String,String> data = new HashMap<>();

    public HbaseRecord(String rowkey, Map<String, String> data) {
        this(rowkey,System.currentTimeMillis(),data);
    }

    public HbaseRecord(String rowkey, Long version, Map<String, String> data) {
        this.rowkey = rowkey;
        this.version = version;
        this.data = data;
    }
    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "HbaseRecord{" +
                "rowkey='" + rowkey + '\'' +
                ", version=" + version +
                ", data=" + data +
                '}';
    }
}
