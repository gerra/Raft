package nio;

import nio.request.Request;

public class EventRequestWrapper implements EventWrapper {
    private Request request;
    private Callback callback;

    public EventRequestWrapper(Request request, Callback callback) {
        this.request = request;
        this.callback = callback;
    }

    public Request getRequest() {
        return request;
    }

    public Callback getCallback() {
        return callback;
    }

    @Override
    public Transferable getTransferable() {
        return request;
    }
}
