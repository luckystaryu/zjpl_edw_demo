package kakfka;

import canal.BaseCanalClientTest;

/**
 * Kafka 测试基类
 */
public abstract class AbstractKafkaTest extends BaseCanalClientTest {
    public static String topic ="test01";
    public static Integer partition =null;
    public static String groupId = "g4";
    public static String servers ="172.16.1.61:6667,172.16.1.62:6667,172.16.1.63:6667";
    public static String zkServers ="172.16.1.61:2181,172.16.1.62:2181,172.16.1.63:2181";

    public void sleep(long time){
        try{
            Thread.sleep(time);
        }catch (InterruptedException e){

        }
    }
}
