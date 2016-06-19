package nio;

import raft.server.Address;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 14.06.16.
 */
public class Properties {

    public static class ServerDescr {
        public final int id;
        public final Address address;

        public ServerDescr(int id, String ip, int port) {
            this.id = id;
            this.address = new Address(ip, port);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ServerDescr that = (ServerDescr) o;

            return id == that.id && (address.equals(that.address));

        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + (address.hashCode());
            return result;
        }
    }

    public final List<ServerDescr> serverDescrs = new ArrayList<>();
    public final long timeout;

    public Properties(String filename) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String s = reader.readLine();
        long timeout = 250;
        while (s != null) {
            if (!s.isEmpty()) {
                String[] property = s.split("=");
                String key = property[0];
                String value = property[1];
                if (key.startsWith("node")) {
                    int id = Integer.parseInt(key.split("\\.")[1]);
                    String[] address = value.split(":");
                    String ip = address[0];
                    int port = Integer.valueOf(address[1]);
                    serverDescrs.add(new ServerDescr(id, ip, port));
                } else if (key.equals("timeout")) {
                    timeout = Long.parseLong(value);
                }
            }
            s = reader.readLine();
        }
        this.timeout = timeout;
    }

    public ServerDescr getServerDescrById(int id) {
        for (ServerDescr serverDescr : serverDescrs) {
            if (serverDescr.id == id) {
                return serverDescr;
            }
        }
        throw new IllegalArgumentException("Server with id " + id + " not found");
    }
}
