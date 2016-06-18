package nio.response;

/**
 * Created by root on 16.06.16.
 */
public class AcceptNodeResponse extends Response {
    public static AcceptNodeResponse fromString(String s) {
        assert s.equals("ACCEPTED");
        return new AcceptNodeResponse();
    }

    @Override
    public String asString() {
        return "ACCEPTED";
    }
}
