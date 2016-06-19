package nio.response;

/**
 * Created by root on 20.06.16.
 */
public class InvalidRequestResponse extends Response {
    @Override
    public String asString() {
        return "INVALID_REQUEST";
    }
}
