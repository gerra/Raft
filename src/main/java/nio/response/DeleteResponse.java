package nio.response;

/**
 * Created by root on 17.06.16.
 */
public class DeleteResponse extends Response {
    private boolean deleted;

    public DeleteResponse(boolean deleted) {
        this.deleted = deleted;
    }

    public static DeleteResponse fromString(String s) {
        if (s.charAt(0) == 'D') {
            return new DeleteResponse(true);
        }
        return new DeleteResponse(false);
    }

    @Override
    public String asString() {
        return deleted ? "DELETED" : "NOT_FOUND";
    }

    public boolean isDeleted() {
        return deleted;
    }
}
