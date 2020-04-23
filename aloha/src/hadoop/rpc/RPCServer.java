package hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @author xzw
 * 2020-04-22
 */
public class RPCServer implements Bizable {

    @Override
    public String talk(String name) {
        return "Hello " + name;
    }

    public static void main(String[] args) throws IOException {
        RPC.Server server = new RPC.Builder(new Configuration())
                .setInstance(new RPCServer())
                .setProtocol(Bizable.class)
                .setBindAddress("localhost")
                .setPort(9501)
                .build();
        server.start();
    }
}
