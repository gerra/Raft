package raft.statemachine;

import java.io.IOException;
import java.io.Reader;

/**
 * Created by root on 13.06.16.
 */
public abstract class StateMachineCommand {
    protected static final String COMMAND_KEY = "command";

    private Boolean result = null;

    public boolean apply(StateMachine stateMachine) {
        result = simpleApply(stateMachine);
        return result;
    }

    protected abstract boolean simpleApply(StateMachine stateMachine);

    public static StateMachineCommand read(Reader reader, int c) throws IOException {
        for (int i = 0; i < COMMAND_KEY.length(); i++) {
            char t = COMMAND_KEY.charAt(i);
            assert t == c;
            c = reader.read();
        }
        assert c == '=';
        c = reader.read();
        assert (c == 's' || c == 'd');
        if (c == 's') {
            return SetCommand.read(reader, c);
        } else {
            return DeleteCommand.read(reader, c);
        }
    }

    @Override
    public String toString() {
        return "command=" + asString();
    }

    public abstract String asString();

    public Boolean getResult() {
        return result;
    }

}
