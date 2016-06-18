package nio.response;

import nio.Helper;
import nio.request.RequestVoteRequest;

/**
 * Created by root on 17.06.16.
 */
public class RequestVoteResponse extends Response {
    private int term;
    private boolean voteGranted;

    public RequestVoteResponse(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public static RequestVoteResponse fromString(String s) {
        return Helper.gson.fromJson(s, RequestVoteResponse.class);
    }

    @Override
    public String asString() {
        return Helper.gson.toJson(this, RequestVoteResponse.class);
    }

    public int getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }
}
