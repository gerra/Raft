package raft;

import nio.NioServer;
import nio.Properties;
import raft.server.Server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 14.06.16.
 */
public class Cluster {
    private List<Server> servers = new ArrayList<>();

    public Cluster(Properties properties) {
//        for (Properties.ServerDescr serverDescr : properties.serverDescrs) {
//            try {
//                Server server = new Server(serverDescr.id, properties, serverDescr.id == 1 ? Server.Role.LEADER : Server.Role.FOLLOWER);
//                servers.add(server);
//                NioServer nioServer = new NioServer(serverDescr, properties, serverDescr.id == 1 ? NioServer.Role.LEADER : Server.Role.FOLLOWER);
//            } catch (IOException ignored) {}
//        }
//        for (Server server : servers) {
//            server.start();
//        }
    }
}
