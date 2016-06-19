package raft.server.request;

import java.io.IOException;
import java.io.Writer;

/**
 * Created by root on 14.06.16.
 */
public interface Transferred {
    void writeToWriter(Writer writer) throws IOException;
}
