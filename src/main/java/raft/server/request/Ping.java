package raft.server.request;

import raft.Utils;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

/**
 * Created by root on 14.06.16.
 */
public class Ping extends Request {

    public Ping(int port) {
        super(port);
    }

    public Ping() {
        this(-1);
    }

    public static Ping read(Reader reader) throws IOException {
        try {
            char[] buf = new char[4];
            reader.read(buf, 0, 3);
//            assert buf[0] == 'p';
            assert buf[0] == 'i';
            assert buf[1] == 'n';
            assert buf[2] == 'g';
        } catch (IOException e) {
            throw e;
        }
        return new Ping();
    }

    @Override
    public String toString() {
        return "ping";
    }

    @Override
    public void writeToWriter(Writer writer) {
        try {
            writer.write("ping");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class Result extends Response {
        public static Result read(Reader reader) throws IOException {
            char[] buf = new char[4];
            reader.read(buf, 0, 4);
            Utils.checkStringAndBufEqual("PONG", buf);
            return new Result();
        }

        @Override
        public void writeToWriter(Writer writer) throws IOException {
            writer.write("PONG");
            writer.flush();
        }
    }
}
