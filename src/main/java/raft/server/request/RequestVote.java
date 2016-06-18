package raft.server.request;

import com.google.gson.Gson;

import java.io.Writer;

/**
 * Created by root on 14.06.16.
 */
public class RequestVote extends Request {
    public final int term;
    public final int candidateId;
    public final int lastLogIndex;
    public final int lastLogTerm;

    public RequestVote(
            int port,
            int term,
            int candidateId,
            int lastLogIndex,
            int lastLogTerm
    ) {
        super(port);
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public RequestVote(int term, int candidateId, int lastLogIndex, int lastLogTerm) {
        this(-1, term, candidateId, lastLogIndex, lastLogTerm);
    }

    @Override
    public void writeToWriter(Writer writer) {
        Gson gson = new Gson();
        gson.toJson(this, RequestVote.class, writer);
    }

    public static class Result extends Response {
        public final int term;
        public final boolean voteGranted;

        public Result(int term, boolean voteGranted) {
            this.term = term;
            this.voteGranted = voteGranted;
        }

        @Override
        public void writeToWriter(Writer writer) {
            Gson gson = new Gson();
            gson.toJson(this, Result.class, writer);
        }
    }
}
