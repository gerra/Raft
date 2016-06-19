package raft.statemachine;

import raft.Utils;

import java.io.IOException;
import java.io.Reader;

/**
 * Created by root on 13.06.16.
 */
public class SetCommand extends StateMachineCommand {
    public final String name;
    public final String value;

    public SetCommand(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public static SetCommand read(Reader reader, int c) throws IOException {
        char[] buf = new char[3];
        buf[0] = (char) c;
        reader.read(buf, 1, 2);
        Utils.checkStringAndBufEqual("set", buf);
        c = reader.read();
        c = Utils.skipSpaces(reader, c);
        String name = "";
        while (c > 0 && !Character.isSpaceChar(c)) {
            name += (char) c;
            c = reader.read();
        }
        c = Utils.skipSpaces(reader, c);
        assert c == '\"';
        int balance = 1;
        String value = "";
        while (balance != 0) {
            c = reader.read();
            if (c == '\"') {
                balance--;
            }
            if (balance != 0) {
                value += (char) c;
            }
        }
        c = reader.read();
        Utils.skipRN(reader, c);
        return new SetCommand(name, value);
    }

    @Override
    public boolean simpleApply(StateMachine stateMachine) {
        assert stateMachine instanceof MyStateMachine;
        return ((MyStateMachine) stateMachine).set(name, value);
    }

    @Override
    public String asString() {
        return "set " + name + " " + value;
    }
}
