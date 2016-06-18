package nio.request;

import nio.Helper;
import nio.model.LogEntry;

import java.util.List;

/**
 * Created by root on 16.06.16.
 */
public class AppendEntriesRequest extends Request {
    public static final String COMMAND = "appendEntries";

    private int term;
    private int leaderId;
    private int prevLogIndex;
    private int prevLogTerm;
    private List<LogEntry> entries;
    private int leaderCommit;

    public AppendEntriesRequest(int term, int leaderId, int prevLogIndex, int prevLogTerm, List<LogEntry> entries, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public static AppendEntriesRequest fromString(String s) {
        return Helper.gson.fromJson(s.substring(COMMAND.length() + 1), AppendEntriesRequest.class);
    }

    @Override
    public String asString() {
        return COMMAND + " " + Helper.gson.toJson(this, AppendEntriesRequest.class);
    }

    public int getTerm() {
        return term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }
}
