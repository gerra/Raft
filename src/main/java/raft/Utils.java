package raft;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import raft.statemachine.DeleteCommand;
import raft.statemachine.SetCommand;
import raft.statemachine.StateMachineCommand;

import java.io.IOException;
import java.io.Reader;

/**
 * Created by root on 14.06.16.
 */
public final class Utils {
    public static int skipSpaces(Reader reader, int c) throws IOException {
        while (Character.isSpaceChar(c) || c == '\n') {
            c = reader.read();
        }
        return c;
    }

    public static int skipRN(Reader reader, int c) throws IOException {
        if (c == '\r') {
            assert reader.read() == '\n';
            return '\n';
        }
        return c;
    }

    public static String getJson(Reader reader) throws IOException {
        StringBuilder sb = new StringBuilder();
        try {
            char[] buf = new char[1024];
            int c = reader.read(buf);
            while (c > 0) {
                if (buf[c - 1] == 0) c--;
                sb.append(buf, 0, c);
                if (c < 1024 && buf[c] == 0) break;
                c = reader.read(buf);
            }
        } catch (Exception e) {
            throw e;
        }
        return sb.toString();
    }

    public static void checkStringAndBufEqual(String s, char[] buf) {
        assert buf.length >= s.length();
        for (int i = 0; i < s.length(); i++) {
            assert(s.charAt(i) == buf[i]);
        }
    }


    public static Gson gson;

    static {
        gson = new GsonBuilder()
                .registerTypeAdapter(StateMachineCommand.class, new TypeAdapter<StateMachineCommand>() {

                    @Override
                    public void write(JsonWriter jsonWriter, StateMachineCommand stateMachineCommand) throws IOException {
                        jsonWriter.beginObject();
                        if (stateMachineCommand instanceof DeleteCommand) {
                            jsonWriter.name("command").value("delete");
                            jsonWriter.name("name").value(((DeleteCommand) stateMachineCommand).name);
                        } else if (stateMachineCommand instanceof SetCommand) {
                            jsonWriter.name("command").value("set");
                            jsonWriter.name("name").value(((SetCommand) stateMachineCommand).name);
                            jsonWriter.name("value").value(((SetCommand) stateMachineCommand).value);
                        }
                        jsonWriter.endObject();
                    }

                    @Override
                    public StateMachineCommand read(JsonReader jsonReader) throws IOException {
                        jsonReader.beginObject();
                        StateMachineCommand result = null;
                        if (jsonReader.nextName().equals("command")) {
                            String command = jsonReader.nextString();
                            if (command.equals("delete")) {
                                jsonReader.nextName();
                                String name = jsonReader.nextString();
                                result = new DeleteCommand(name);
                            } else {
                                jsonReader.nextName();
                                String name = jsonReader.nextString();
                                jsonReader.nextName();
                                String value = jsonReader.nextString();
                                result = new SetCommand(name, value);
                            }
                        }
                        jsonReader.endObject();
                        return result;
                    }
                })
                .create();
    }

}
