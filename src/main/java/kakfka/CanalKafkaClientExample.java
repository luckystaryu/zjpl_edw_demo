package kakfka;

import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Kafka client example
 */
public class CanalKafkaClientExample {
    protected final static Logger logger = LoggerFactory.getLogger(CanalKafkaClientExample.class);
    private KafkaCanalConnector connector;
    private static volatile boolean  running = false;
    private Thread thread = null;
    private Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler(){
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          logger.error("parse events has an error ",e);
        }
    };
    public CanalKafkaClientExample(String zkServers,String servers,String topic,Integer partition,String groupId){
        connector = new KafkaCanalConnector(servers,topic,partition,groupId,null,false);
    }
    public static void main(String[] args){
        try{
            final CanalKafkaClientExample kafkaCanalClientExample = new CanalKafkaClientExample(AbstractKafkaTest.zkServers,
                    AbstractKafkaTest.servers,
                    AbstractKafkaTest.topic,
                    AbstractKafkaTest.partition,
                    AbstractKafkaTest.groupId);
            logger.info("## start the kafka consumer:{}-{}",AbstractKafkaTest.topic,AbstractKafkaTest.groupId);
            kafkaCanalClientExample.start();
        }catch (Throwable e){
            logger.error("");
            System.exit(0);
        }
    }

    private void start() {
        Assert.notNull(connector,"connector is null");
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                process();
            }
        });
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }
    public void stop(){
        if(!running){
            return;
        }
        running = false;
        if(thread !=null){
            try {
                thread.join();
            } catch (InterruptedException e) {
            }
        }
    }

    private void process() {
        while(!running){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        while(running){
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    List<Message> messages = connector.getListWithoutAck(100L, TimeUnit.MILLISECONDS); //获取message
                    if (messages == null) {
                        continue;
                    }
                    for (Message message : messages) {
                        long batchId = message.getId();
                        int size = message.getEntries().size();
                        if (batchId == -1 || size == 0) {

                        } else {
                            logger.info(message.toString());
                        }
                    }
                    connector.ack();//提交确认
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
