package raft.server;

/**
 * Created by root on 14.06.16.
 */
public class Address {
    public final String ip;
    public final int port;

    public Address(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Address address = (Address) o;

        return port == address.port && (ip != null ? ip.equals(address.ip) : address.ip == null);

    }

    @Override
    public int hashCode() {
        int result = ip != null ? ip.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }
}
