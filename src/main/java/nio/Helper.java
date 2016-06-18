package nio;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import nio.model.DeleteCommand;
import nio.model.SetCommand;
import nio.model.StateMachineCommand;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by root on 16.06.16.
 */
public class Helper {
    public static Gson gson = new GsonBuilder()
            .registerTypeAdapter(StateMachineCommand.class, new TypeAdapter<StateMachineCommand>() {
                @Override
                public void write(JsonWriter jsonWriter, StateMachineCommand stateMachineCommand) throws IOException {
                    jsonWriter.beginObject();
                    if (stateMachineCommand instanceof DeleteCommand) {
                        jsonWriter.name("command").value("delete");
                        jsonWriter.name("name").value(((DeleteCommand) stateMachineCommand).getKey());
                    } else if (stateMachineCommand instanceof SetCommand) {
                        jsonWriter.name("command").value("set");
                        jsonWriter.name("name").value(((SetCommand) stateMachineCommand).getKey());
                        jsonWriter.name("value").value(((SetCommand) stateMachineCommand).getValue());
                    }
                    jsonWriter.endObject();
                }

                @Override
                public StateMachineCommand read(JsonReader jsonReader) throws IOException {
                    StateMachineCommand result = null;
                    jsonReader.beginObject();
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

    public static int skipSpaces(InputStream is) throws IOException {
        int c = is.read();
        while (Character.isSpaceChar(c)) {
            c = is.read();
        }
        return c;
    }

}
