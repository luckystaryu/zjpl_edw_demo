package canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

import java.net.InetSocketAddress;

public class SimpleCanalClientDemo  extends AbstarctCanalClientTest{
    public SimpleCanalClientDemo(String destination) {
        super(destination);
    }

    public static void main(String[] args) {
        String destination = "example";
        String ip ="172.16.1.61";
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip,11111),
                destination,
                "canal",
                "canal");
        final SimpleCanalClientDemo client = new SimpleCanalClientDemo(destination);
        client.setConnector(connector);
        client.start();
    }
}
