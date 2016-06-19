package raft;

import raft.statemachine.StateMachineCommand;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Created by root on 13.06.16.
 */
public class LogEntry {
    private static final String TERM_KEY = "term";
    private static final String COMMAND_KEY = "command";
    private static final String ARGS_KEY = "args";

    public final int term;
    public final StateMachineCommand command;

    public LogEntry(int term, StateMachineCommand command) {
        this.term = term;
        this.command = command;
    }

    public static LogEntry read(BufferedReader reader, int c) throws IOException {
        int term = 0;
        for (int i = 0; i < TERM_KEY.length(); i++) {
            char t = TERM_KEY.charAt(i);
            assert c == t;
            c = reader.read();
        }
        c = Utils.skipSpaces(reader, c);
        assert c == '=';
        Utils.skipSpaces(reader, c);
        c = reader.read();
        while (Character.isDigit(c)) {
            term = (term * 10) + (c - '0');
            c = reader.read();
        }
        c = Utils.skipSpaces(reader, c);
        StateMachineCommand command = StateMachineCommand.read(reader, c);
        return new LogEntry(term, command);
    }

    @Override
    public String toString() {
        return TERM_KEY + "=" + term + " " + command.toString();
    }
}
