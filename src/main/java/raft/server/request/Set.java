package raft.server.request;

import raft.Utils;
import raft.statemachine.DeleteCommand;
import raft.statemachine.SetCommand;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

/**
 * Created by root on 15.06.16.
 */
public class Set extends Request {
    public final String name;
    public final String value;

    protected Set(int port, String name, String value) {
        super(port);
        this.name = name;
        this.value = value;
    }

    public Set(String name, String value) {
        this(-1, name, value);
    }


    public static Set read(Reader reader) throws IOException {
        SetCommand setCommand = SetCommand.read(reader, 's');
        return new Set(setCommand.name, setCommand.value);
    }

    @Override
    public void writeToWriter(Writer writer) throws IOException {
        writer.write(new SetCommand(name, value).asString());
        writer.write(0);
        writer.flush();
    }

    public static class Result extends Response {

        public static Result read(Reader reader) throws IOException {
            char[] buf = new char[6];
            reader.read(buf, 0, 6);
            Utils.checkStringAndBufEqual("STORED", buf);
            return new Result();
        }

        @Override
        public void writeToWriter(Writer writer) throws IOException {
            writer.write("STORED");
            writer.flush();
        }
    }
}
