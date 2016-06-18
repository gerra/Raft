package nio.response;

/**
 * Created by root on 16.06.16.
 */
public class PingResponse extends Response {

    public static PingResponse fromString(String s) {
        assert s.equals("PONG");
        return new PingResponse();
    }

    @Override
    public String asString() {
        return "PONG";
    }
}
