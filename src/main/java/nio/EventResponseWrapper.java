package nio;

import nio.response.Response;

/**
 * Created by root on 16.06.16.
 */
public class EventResponseWrapper extends EventWrapper {
    private Response response;

    public EventResponseWrapper(Response response, Callback callback) {
        super(callback);
        this.response = response;
    }

    public Response getResponse() {
        return response;
    }
}
