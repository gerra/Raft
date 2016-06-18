package nio;

import java.util.ArrayDeque;
import java.util.Queue;

public class SelectKeyExtraInfo {
    /**
     * server client or just user
     */
    private boolean isClient;
    /**
     * id of server client or -1 in the case if just user
     */
    private Queue<EventWrapper> queue = new ArrayDeque<>();

    private int remoteServerId = -1;

    private EventWrapper lastEventWrapper;

    public SelectKeyExtraInfo(boolean isClient) {
        this.isClient = isClient;
    }

    public void addEventWrapper(EventWrapper eventWrapper) {
        queue.add(eventWrapper);
    }

    public boolean isClient() {
        return isClient;
    }

    public Queue<EventWrapper> getQueue() {
        return queue;
    }

    public void setLastEventWrapper(EventWrapper lastEventWrapper) {
        this.lastEventWrapper = lastEventWrapper;
    }

    public EventWrapper getLastEventWrapper() {
        return lastEventWrapper;
    }

    public int getRemoteServerId() {
        return remoteServerId;
    }

    public void setRemoteServerId(int remoteServerId) {
        this.remoteServerId = remoteServerId;
    }
}
