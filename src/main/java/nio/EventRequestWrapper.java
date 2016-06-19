package nio;

import nio.request.Request;

public class EventRequestWrapper extends EventWrapper {
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
}
