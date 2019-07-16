//package canal;
//
//import com.alibaba.otter.canal.client.CanalConnector;
//import com.alibaba.otter.canal.client.CanalConnectors;
//
//import java.net.InetSocketAddress;
//
//public class SimpleCanalClientTest extends AbstarctCanalClientTest {
//    public SimpleCanalClientTest(String destination) {
//        super(destination);
//    }
//
//    public static void main(String[] args) {
//        //根据ip,直接创建连接，无HA的功能
//        String destination ="example";
//        String ip="172.16.1.61";
//        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip,11111),
//                destination,
//                "",
//                "");
//        final SimpleCanalClientTest clientTest =new SimpleCanalClientTest(destination);
//        clientTest.setConnector(connector);
//        clientTest.start();
//        Runtime.getRuntime().addShutdownHook(new Thread(){
//            public void run(){
//                try{
//                    logger.info("## stop the canal client");
//                    clientTest.stop();
//                }catch (Throwable e){
//                    logger.warn("##something goes wrong when stopping canal:",e);
//                }finally {
//                    logger.info("##canal cleint is down.");
//                }
//            }
//        });
//    }
//}
