package nio;

import nio.response.Response;

/**
 * Created by root on 16.06.16.
 */
public class Callback<T extends Response> {
    public void onSuccess(T result) {}
    public void onError(Object object) {}

}
