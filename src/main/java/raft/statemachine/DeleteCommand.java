package raft.statemachine;

import raft.Utils;

import java.io.IOException;
import java.io.Reader;

/**
 * Created by root on 14.06.16.
 */
public class DeleteCommand extends StateMachineCommand {
    public final String name;

    public DeleteCommand(String name) {
        this.name = name;
    }

    public static DeleteCommand read(Reader reader, int c) throws IOException {
        char[] buf = new char[6];
        buf[0] = (char) c;
        reader.read(buf, 1, 5);
        Utils.checkStringAndBufEqual("delete", buf);
        c = reader.read();
        c = Utils.skipSpaces(reader, c);
        String name = "";
        while (c > 0 && !Character.isSpaceChar(c) && c != '\n' && c != '\r') {
            name += (char) c;
            c = reader.read();
        }
        Utils.skipRN(reader, c);
        return new DeleteCommand(name);
    }

    public static DeleteCommand parse(String argsAsString) {
        return new DeleteCommand(argsAsString);
    }

    @Override
    public boolean simpleApply(StateMachine stateMachine) {
        assert stateMachine instanceof MyStateMachine;
        return ((MyStateMachine) stateMachine).delete(name);
    }

    @Override
    public String asString() {
        return "delete " + name;
    }
}
