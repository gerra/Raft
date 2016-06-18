package raft.server.request;

import java.io.IOException;
import java.io.Writer;

/**
 * Created by root on 14.06.16.
 */
public abstract class Response implements Transferred {
    public static class Error extends Response {

        @Override
        public void writeToWriter(Writer writer) throws IOException {
            writer.write("error");
            writer.flush();
        }
    }
}
