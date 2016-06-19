package nio.response;

import nio.Helper;

/**
 * Created by root on 16.06.16.
 */
public class AppendEntriesResponse extends Response {
    private int term;
    private boolean success;

    public AppendEntriesResponse(int term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public static AppendEntriesResponse fromString(String s) {
        return Helper.gson.fromJson(s, AppendEntriesResponse.class);
    }

    @Override
    public String asString() {
        return Helper.gson.toJson(this, AppendEntriesResponse.class);
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }
}
