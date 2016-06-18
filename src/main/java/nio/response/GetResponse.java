package nio.response;

/**
 * Created by root on 17.06.16.
 */
public class GetResponse extends Response {
    private String key;
    private String value;

    public GetResponse(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public GetResponse(boolean notFound) {
        this.key = null;
        this.value = null;
    }

    public static GetResponse fromString(String s) {
        if (s.charAt(0) == 'V') {
            int i = 6;
            while (Character.isSpaceChar(s.charAt(i)) || s.charAt(i) == '\n') {
                i++;
            }
            String key = "";
            while (!Character.isSpaceChar(s.charAt(i))) {
                key += s.charAt(i);
                i++;
            }
            while (Character.isSpaceChar(s.charAt(i)) || s.charAt(i) == '\n') {
                i++;
            }
            String value = s.substring(i + 1, s.length() - 1);
            return new GetResponse(key, value);
        } else {
            return new GetResponse(false);
        }
    }

    @Override
    public String asString() {
        if (key != null) {
            return "VALUE " + key + " " + value;
        }
        return "NOT_FOUND";
    }
}
