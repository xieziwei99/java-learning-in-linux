package hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @author xzw
 * 2020-04-23
 */
public class RPCServer2 implements Bizable {

    @Override
    public String talk(String name) {
        return "What's your name? " + name;
    }

    public static void main(String[] args) throws IOException {
        RPC.Server server = new RPC.Builder(new Configuration())
                .setInstance(new RPCServer2())
                .setProtocol(Bizable.class)
                .setBindAddress("localhost")
                .setPort(9502)
                .build();
        server.start();
    }
}
