package raft.statemachine;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 13.06.16.
 */
public class MyStateMachine extends StateMachine {
    private Map<String, String> storage = new HashMap<>();

    public boolean set(String name, String value) {
        storage.put(name, value);
        return true;
    }

    public boolean delete(String name) {
        String prevValue = storage.remove(name);
        return prevValue != null;
    }

    public String get(String name) {
        return storage.get(name);
    }
}
