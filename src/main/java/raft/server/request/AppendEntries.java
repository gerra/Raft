package raft.server.request;

import com.google.gson.Gson;
import raft.LogEntry;
import raft.Utils;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

public class AppendEntries extends Request {
    public final int term;
    public final int leaderId;
    public final int prevLogIndex;
    public final int prevLogTerm;
    public final List<LogEntry> entries;
    public final int leaderCommit;

    public static AppendEntries read(Reader reader) throws IOException {
        String asString = Utils.getJson(reader);
        AppendEntries result = Utils.gson.fromJson(asString, AppendEntries.class);
        return result;
    }

    @Override
    public void writeToWriter(Writer writer) throws IOException {
        writer.write('a');
        Utils.gson.toJson(this, AppendEntries.class, writer);
        writer.flush();
    }

    public AppendEntries(
            int port,
            int term,
            int leaderId,
            int prevLogIndex,
            int prevLogTerm,
            List<LogEntry> entries,
            int leaderCommit
    ) {
        super(port);
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public AppendEntries(
            int term,
            int leaderId,
            int prevLogIndex,
            int prevLogTerm,
            List<LogEntry> entries,
            int leaderCommit
    ) {
        this(-1, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    public static class Result extends Response {
        public final int term;
        public final boolean success;

        public Result(boolean success, int term) {
            this.success = success;
            this.term = term;
        }

        public static Result read(Reader reader) throws IOException {
            Gson gson = new Gson();
            return gson.fromJson(Utils.getJson(reader), Result.class);
        }

        @Override
        public void writeToWriter(Writer writer) throws IOException {
            Gson gson = new Gson();
            gson.toJson(this, Result.class, writer);
            writer.flush();
        }
    }
}
