package nio.request;

/**
 * Created by root on 17.06.16.
 */
public class DeleteRequest extends Request implements LeaderOnly {
    private String key;

    public DeleteRequest(String key) {
        this.key = key;
    }

    public static DeleteRequest fromString(String s) {
        int i = 7;
        return new DeleteRequest(s.substring(i).trim());
    }

    @Override
    public String asString() {
        return "delete " + key;
    }

    public String getKey() {
        return key;
    }
}
