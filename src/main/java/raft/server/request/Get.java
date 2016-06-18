package raft.server.request;

import raft.Utils;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

/**
 * Created by root on 16.06.16.
 */
public class Get extends Request {
    public final String name;

    protected Get(int port, String name) {
        super(port);
        this.name = name;
    }

    public Get(String name) {
        this(-1, name);
    }

    public static Get read(Reader reader) throws IOException {
        assert reader.read() == 'e';
        assert reader.read() == 't';
        int c = reader.read();
        c = Utils.skipSpaces(reader, c);
        String name = "";
        while (c > 0 && !Character.isSpaceChar(c) && c != '\n' && c != '\r') {
            name += (char) c;
            c = reader.read();
        }
        Utils.skipRN(reader, c);
        return new Get(name);
    }

    @Override
    public void writeToWriter(Writer writer) throws IOException {
        writer.write("get");
        writer.write(0);
        writer.flush();
    }

    public static class Result extends Response {
        public final String key;
        public final String value;

        public Result(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void writeToWriter(Writer writer) throws IOException {
            writer.write(value == null ? "NOT_FOUND" : "VALUE " + key + " " + value);
            writer.flush();
        }
    }
}
