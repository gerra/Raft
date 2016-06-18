package nio.request;

/**
 * Created by root on 16.06.16.
 */
public class AcceptNodeRequest extends Request {
    private int id;

    public AcceptNodeRequest(int id) {
        this.id = id;
    }

    public static Request fromString(String s) {
        int id = Integer.parseInt(s.split("[\\s]+")[1]);
        return new AcceptNodeRequest(id);
    }

    public int getId() {
        return id;
    }

    @Override
    public String asString() {
        return "node " + id;
    }
}
