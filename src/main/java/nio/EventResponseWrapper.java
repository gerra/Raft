package nio;

import nio.response.Response;

/**
 * Created by root on 16.06.16.
 */
public class EventResponseWrapper extends EventWrapper {
    private Response response;

    public EventResponseWrapper(Response response) {
        this.response = response;
    }

    public Response getResponse() {
        return response;
    }
}
