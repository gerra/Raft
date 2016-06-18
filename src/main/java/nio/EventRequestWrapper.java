package nio;

import nio.request.Request;

public class EventRequestWrapper extends EventWrapper {
    private Request request;

    public EventRequestWrapper(Request request, Callback callback) {
        super(callback);
        this.request = request;
    }

    public Request getRequest() {
        return request;
    }
}
