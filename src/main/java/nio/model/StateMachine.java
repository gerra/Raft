package nio.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 16.06.16.
 */
public class StateMachine {
    private HashMap<String, String> storage = new HashMap<>();

    public void set(String key, String value) {
        storage.put(key, value);
    }

    public String get(String key) {
        return storage.get(key);
    }

    public boolean delete(String key) {
        return storage.remove(key) != null;
    }
}
