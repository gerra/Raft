package nio;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws Exception {
        Log.disableDebug();
        Properties properties = new Properties("dkvs.properties");
        int nodeId = Integer.parseInt(args[0]);
        for (Properties.ServerDescr serverDescr : properties.serverDescrs) {
            try {
                if (nodeId == serverDescr.id) {
                    NioServer nioServer = new NioServer(serverDescr, properties, NioServer.Role.FOLLOWER);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
