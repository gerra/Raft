package nio;

/**
 * Created by root on 16.06.16.
 */
public abstract class EventWrapper {
    private Callback callback;

    public EventWrapper(Callback callback) {
        this.callback = callback;
    }

    public Callback getCallback() {
        return callback;
    }
}
