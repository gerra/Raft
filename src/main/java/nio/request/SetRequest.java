package nio.request;

/**
 * Created by root on 16.06.16.
 */
public class SetRequest extends Request implements LeaderOnly {
    private String key;
    private String value;

    public SetRequest(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public static SetRequest fromString(String s) {
        int i = 4;
        while (Character.isSpaceChar(s.charAt(i)) || s.charAt(i) == '\n') {
            i++;
        }
        String name = "";
        while (!Character.isSpaceChar(s.charAt(i))) {
            name += s.charAt(i);
            i++;
        }
        while (Character.isSpaceChar(s.charAt(i)) || s.charAt(i) == '\n') {
            i++;
        }
        String value = s.substring(i + 1, s.length() - 1);
        return new SetRequest(name, value);
    }

    @Override
    public String asString() {
        return "set " + key + " \"" + value + "\"";
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
