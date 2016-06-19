package nio;

import java.util.ArrayDeque;
import java.util.Queue;

public class SelectKeyExtraInfo {
    private boolean isClient;
    private boolean isAccepted;

    private Queue<EventWrapper> outputQueue = new ArrayDeque<>();
    private Queue<EventRequestWrapper> responseQueue = new ArrayDeque<>();

    private int remoteServerId = -1;

    public SelectKeyExtraInfo(boolean isClient) {
        this.isClient = isClient;
    }

    public void addEventWrapper(EventWrapper eventWrapper) {
        outputQueue.add(eventWrapper);
    }

    /**
     * @return
     * {@code true} if related connection is for process requests and response to them
     * <br/>
     * {@code false} if related connection is for make request and process responses for them
     */
    public boolean isClient() {
        return isClient;
    }

    public Queue<EventWrapper> getOutputQueue() {
        return outputQueue;
    }

    public Queue<EventRequestWrapper> getResponseQueue() {
        return responseQueue;
    }

    public int getRemoteServerId() {
        return remoteServerId;
    }

    public void setRemoteServerId(int remoteServerId) {
        this.remoteServerId = remoteServerId;
    }

    /**
     * Only for to server connection
     * @return
     * {@code true} if server accepted us
     * <br/>
     * {@code false} if server didn't accept us
     */
    public boolean isAccepted() {
        return isAccepted;
    }

    public void setAccepted(boolean accepted) {
        isAccepted = accepted;
    }
}
