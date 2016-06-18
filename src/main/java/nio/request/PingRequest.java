package nio.request;

import sun.dc.pr.PRError;

/**
 * Created by root on 16.06.16.
 */
public class PingRequest extends Request {

    public static PingRequest fromString(String s) {
        assert s.equals("ping");
        return new PingRequest();
    }

    @Override
    public String asString() {
        return "ping";
    }
}
