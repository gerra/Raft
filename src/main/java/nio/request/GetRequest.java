package nio.request;

/**
 * Created by root on 17.06.16.
 */
public class GetRequest extends Request {
    private String key;

    public GetRequest(String key) {
        this.key = key;
    }

    public static GetRequest fromString(String s) {
        int i = 4;
        return new GetRequest(s.substring(i).trim());
    }

    @Override
    public String asString() {
        return "get " + key;
    }

    public String getKey() {
        return key;
    }
}
