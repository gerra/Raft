package raft.server.request;

/**
 * Created by root on 14.06.16.
 */
public abstract class Callback<T extends Response> {
    public void onPreExecute() {}
    public void onSuccess(T result) {}
    public void onError() {}
}
