package nio.model;

/**
 * Created by root on 16.06.16.
 */
public class LogEntry {
    private int term;
    private StateMachineCommand command;

    public LogEntry(int term, StateMachineCommand command) {
        this.term = term;
        this.command = command;
    }

    public int getTerm() {
        return term;
    }

    public StateMachineCommand getCommand() {
        return command;
    }
}