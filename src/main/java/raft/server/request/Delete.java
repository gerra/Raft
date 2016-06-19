package raft.server.request;

import raft.Utils;
import raft.statemachine.DeleteCommand;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

public class Delete extends Request {
    public final String name;

    protected Delete(int port, String name) {
        super(port);
        this.name = name;
    }

    public Delete(String name) {
        this(-1, name);
    }

    public static Delete read(Reader reader) throws IOException {
        DeleteCommand deleteCommand = DeleteCommand.read(reader, 'd');
        return new Delete(deleteCommand.name);
    }

    @Override
    public void writeToWriter(Writer writer) throws IOException {
        writer.write(new DeleteCommand(name).asString());
        writer.write(0);
        writer.flush();
    }

    public static class Result extends Response {
        private boolean deleted;

        public Result(boolean deleted) {
            this.deleted = deleted;
        }

        public static Result read(Reader reader) throws IOException {
            int c = reader.read();
            assert (c == 'D' || c == 'N');
            String type;
            if (c == 'D') {
                type = "DELETED";
            } else {
                type = "NOT_FOUND";
            }
            char[] buf = new char[type.length()];
            buf[0] = (char) c;
            reader.read(buf, 1, type.length() - 1);
            Utils.checkStringAndBufEqual(type, buf);
            if (c == 'D') {
                return new Result(true);
            } else {
                return new Result(false);
            }
        }

        @Override
        public void writeToWriter(Writer writer) throws IOException {
            if (deleted) {
                writer.write("DELETED");
            } else {
                writer.write("NOT_FOUND");
            }
            writer.flush();
        }
    }
}
