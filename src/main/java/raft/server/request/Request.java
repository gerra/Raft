package raft.server.request;

/**
 * Created by root on 14.06.16.
 */
public abstract class Request implements Transferred {
    /**
     * Useful for incoming request, from exact port
     */
    private transient int port;

    protected Request(int port) {
        this.port = port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }
}