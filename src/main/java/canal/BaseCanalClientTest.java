package canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public abstract class BaseCanalClientTest {
    protected final static Logger logger  = LoggerFactory.getLogger(AbstarctCanalClientTest.class);
    protected static final String SEP     = SystemUtils.LINE_SEPARATOR;
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    protected volatile boolean running       = false;
    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler(){
        public void uncaughtException(Thread t,Throwable e){
            logger.error("parse events has an error",e);
        }
    };
    protected Thread         thread         = null;
    protected CanalConnector connector;
    protected static String  context_format = null;
    protected static String  row_format     = null;
    protected static String  transaction_format = null;
    protected String         destination;
    static {
        context_format = SEP +"********************************************"+SEP;
        context_format ="* Batch Id:[{}],count:[{}],memsize:[{}],Time:{}"+SEP;
        context_format ="* Start:[{}] "+SEP;
        context_format ="* End:[{}] " +SEP;
        context_format ="************************************************"+SEP;

        row_format=SEP
                    +"------------>binlog[{}:{}],name[{},{}],eventType:{},executeTime:{}({}),gtid:({}),delay:{} ms"
                    +SEP;
        transaction_format =SEP
                +"================>binlog[{}:{}],executeTime:{}({}),gtid:({}),delay:{}ms"
                +SEP;
    }
    protected void printSummary(Message message,long batchId,int size){
        long memsize =0;
        for(CanalEntry.Entry entry:message.getEntries()){
            memsize +=entry.getHeader().getEventLength();
            System.out.println(String.valueOf(entry.getHeader())+String.valueOf(entry.getStoreValue()));
        }
        String startPosition = null;
        String endPosition = null;
        if(!CollectionUtils.isEmpty(message.getEntries())){
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size()-1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format,new Object[]{batchId,size,memsize,format.format(new Date()),startPosition,endPosition});
    }

    private String buildPositionForDump(CanalEntry.Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        String position = entry.getHeader().getLogfileName()+":"+entry.getHeader().getLogfileOffset()+":"
                        + entry.getHeader().getExecuteTime()+"("+ format.format(date)+")";
        if(StringUtils.isNotEmpty(entry.getHeader().getGtid())){
            position +="gtid(" + entry.getHeader().getGtid()+")";
        }
        return position;
    }

    protected void printEntry(List<CanalEntry.Entry> entries){
        for(CanalEntry.Entry entry:entries){
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;
            Date date = new Date(entry.getHeader().getExecuteTime());
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN||entry.getEntryType()== CanalEntry.EntryType.TRANSACTIONEND){
                if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN){
                    CanalEntry.TransactionBegin begin = null;
                    try{
                        begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    }catch (InvalidProtocolBufferException e){
                        throw new RuntimeException("parse event has an error,data:"+entry.toString(),e);
                    }
                    //打印事务头信息,执行的线程id,事务耗时
                    logger.info(transaction_format,
                            new Object[]{entry.getHeader().getLogfileName(),
                            String.valueOf(entry.getHeader().getLogfileOffset()),
                            String.valueOf(entry.getHeader().getExecuteTime()),
                            simpleDateFormat.format(date),
                            entry.getHeader().getGtid(),
                            String.valueOf(delayTime)});
                    logger.info(" BEGIN ------> Thread id:{}",begin.getThreadId());
                    printXAInfo(begin.getPropsList());
                }else if(entry.getEntryType()== CanalEntry.EntryType.TRANSACTIONEND){
                    CanalEntry.TransactionEnd end = null;
                    try{
                        end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    }catch (InvalidProtocolBufferException e){
                        throw new RuntimeException("parse event has an error,data:"+entry.toString(),e);
                    }
                    //打印事务提交信息,事务id
                    logger.info("----------------------\n");
                    logger.info(" END-------->transaction id:{}",end.getTransactionId());
                    printXAInfo(end.getPropsList());
                    logger.info(transaction_format,
                            new Object[]{entry.getHeader().getLogfileName(),
                            String.valueOf(entry.getHeader().getLogfileOffset()),
                            String.valueOf(entry.getHeader().getLogfileOffset()),
                            String.valueOf(entry.getHeader().getExecuteTime()),
                            simpleDateFormat.format(date),
                            entry.getHeader().getGtid(),
                            String.valueOf(delayTime)});
                }
                continue;
            }
            if(entry.getEntryType()== CanalEntry.EntryType.ROWDATA){
                CanalEntry.RowChange rowChange =null;
                try{
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                }catch (Exception e){
                    throw new RuntimeException("parse event has an error,data:"+entry.toString(),e);
                }
                CanalEntry.EventType eventType = rowChange.getEventType();
                logger.info(row_format,
                        new Object[]{entry.getHeader().getLogfileName(),
                        String.valueOf(entry.getHeader().getLogfileOffset()),
                        entry.getHeader().getSchemaName(),
                        entry.getHeader().getTableName(),
                        eventType,
                        String.valueOf(entry.getHeader().getExecuteTime()),
                        simpleDateFormat.format(date),
                        entry.getHeader().getGtid(),
                        String.valueOf(delayTime)});

                if(eventType == CanalEntry.EventType.QUERY||rowChange.getIsDdl()){
                    logger.info("sql ---->"+rowChange.getSql()+SEP);
                    continue;
                }
                printXAInfo(rowChange.getPropsList());
                for(CanalEntry.RowData rowData:rowChange.getRowDatasList()){
                    if(eventType == CanalEntry.EventType.DELETE){
                        printColumn(rowData.getBeforeColumnsList());
                    }else if (eventType == CanalEntry.EventType.INSERT){
                        printColumn(rowData.getAfterColumnsList());
                    }else if(eventType == CanalEntry.EventType.UPDATE){
                        logger.info("print before column list >>>>>>>>>>>>>>>>>>>>>>");
                        printColumn(rowData.getBeforeColumnsList());

                        logger.info("print after column list >>>>>>>>>>>>>>>>>>>>>>>");
                        printColumn(rowData.getAfterColumnsList());
                    }
                    else{
                        printColumn(rowData.getAfterColumnsList());
                    }
                }
            }
        }
    }

    private void printColumn(List<CanalEntry.Column> columns) {
        for(CanalEntry.Column column:columns){
            StringBuilder builder = new StringBuilder();
            try{
                if(StringUtils.containsIgnoreCase(column.getMysqlType(),"BLOB")
                        || StringUtils.containsIgnoreCase(column.getMysqlType(),"BINARY")){
                    builder.append(column.getName()+":"
                            + new String(column.getValue().getBytes("ISO-8859-1"),"UTF-8"));
                }else{
                    builder.append(column.getName()+":"+column.getValue());
                }
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            builder.append("    type="+column.getMysqlType());
            if(column.getUpdated()){
                builder.append("   update=" + column.getUpdated());
            }
            builder.append(SEP);
            logger.info(builder.toString());
        }
    }

    private void printXAInfo(List<CanalEntry.Pair> pairs) {
        if(pairs ==null) {
            return;
        }

        String xaType = null;
        String xaXid = null;
        for(CanalEntry.Pair pair:pairs){
            String key = pair.getKey();
            if(StringUtils.endsWithIgnoreCase(key,"XA_TYPE")){
                xaType = pair.getValue();
            }else if (StringUtils.endsWithIgnoreCase(key,"XA_XID")){
                xaXid = pair.getValue();
            }
        }
        if(xaType !=null &&xaXid!= null){
            logger.info("------->"+xaType+""+xaXid);
        }
    }

    public void setConnector(CanalConnector connector){
        this.connector = connector;
    }
    /***
     * 获取当前Entry的GTID信息示例
     */
    public static String getCurrentGtid(CanalEntry.Header header){
        List<CanalEntry.Pair> props = header.getPropsList();
        if(props != null && props.size()>0){
            for(CanalEntry.Pair pair:props){
                if("curtGid".equals(pair.getKey())){
                    return pair.getValue();
                }
            }
        }
        return "";
    }
    /***
     * 获取当前Entry的GTID Sequence No 信息示例
     */
    public static String getCurrentGtidSn(CanalEntry.Header header){
        List<CanalEntry.Pair> props = header.getPropsList();
        if(props !=null && props.size()>0){
            for(CanalEntry.Pair pair:props){
                if("curtGtidSn".equals(pair.getKey())){
                    return pair.getValue();
                }
            }
        }
        return "";
    }
    /**
     * 获取当前的Entry的GTID Last Committed信息示例
     */
    public static String getCurrentGtidLct(CanalEntry.Header header){
        List<CanalEntry.Pair> props = header.getPropsList();
        if(props !=null && props.size()>0){
            for(CanalEntry.Pair pair:props){
                if("curtGtidLct".equals(pair.getKey())){
                    return pair.getValue();
                }
            }
        }
        return "";
    }
}
