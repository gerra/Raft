package nio.request;

import nio.Helper;

/**
 * Created by root on 17.06.16.
 */
public class RequestVoteRequest extends Request {
    public static final String COMMAND = "requestVote";

    private int term;
    private int candidateId;
    private int lastLogIndex;
    private int lastLogTerm;

    public RequestVoteRequest(int term, int candidateId, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public static RequestVoteRequest fromString(String s) {
        return Helper.gson.fromJson(s.substring(COMMAND.length() + 1), RequestVoteRequest.class);
    }

    @Override
    public String asString() {
        return COMMAND + " " + Helper.gson.toJson(this, RequestVoteRequest.class);
    }

    public int getTerm() {
        return term;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }
}
