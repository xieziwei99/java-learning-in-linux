package hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author xzw
 * 2020-04-22
 */
public class RPCClient {

    public static void main(String[] args) throws IOException {
        Bizable proxy = RPC.getProxy(
                Bizable.class,
                Bizable.versionID,
                // 根据 ip + 端口 判断调用哪个 Server
                new InetSocketAddress("localhost", 9502),
                new Configuration()
        );
        System.out.println(proxy.talk("Jack"));
        RPC.stopProxy(proxy);
    }
}
