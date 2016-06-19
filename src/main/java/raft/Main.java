package raft;

import nio.Log;
import nio.NioServer;
import nio.Properties;

import java.io.IOException;

/**
 * Created by root on 14.06.16.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Log.disableDebug();
        Properties properties = new Properties("dkvs.properties");
//        Cluster cluster = new Cluster(properties);
        int nodeId = Integer.parseInt(args[0]);
        for (Properties.ServerDescr serverDescr : properties.serverDescrs) {
            try {
                if (nodeId == serverDescr.id) {
//                Server server = new Server(serverDescr.id, properties, serverDescr.id == 1 ? Server.Role.LEADER : Server.Role.FOLLOWER);
//                servers.add(server);
                    NioServer nioServer = new NioServer(serverDescr, properties, NioServer.Role.FOLLOWER);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
//        for (Server server : servers) {
//            server.start();
//        }
    }
}
