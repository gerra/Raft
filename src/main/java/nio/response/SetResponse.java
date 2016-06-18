package nio.response;

/**
 * Created by root on 16.06.16.
 */
public class SetResponse extends Response {
    public static SetResponse fromString(String s) {
        assert s.equals("STORED");
        return new SetResponse();
    }

    @Override
    public String asString() {
        return "STORED";
    }
}
