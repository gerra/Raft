package nio;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import nio.model.DeleteCommand;
import nio.model.SetCommand;
import nio.model.StateMachineCommand;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

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

    public static ByteBuffer convertStringToByteBuffer(String s) {
        return ByteBuffer.wrap(s.getBytes());
    }

    static String readFromChannel(SocketChannel socketChannel) throws IOException {
        StringBuilder requestBuilder = new StringBuilder();
        ByteBuffer byteBuffer = ByteBuffer.allocate(256);
        int wasRead;
        try {
            while ((wasRead = socketChannel.read(byteBuffer)) > 0) {
                byteBuffer.clear();
                requestBuilder.append(new String(byteBuffer.array(), 0, wasRead));
            }
        } catch (Exception ignored) {
            wasRead = -1;
        }
        byteBuffer.clear();
        if (wasRead < 0) {
            return null;
        }
        return requestBuilder.toString();
    }

    static int writeToChannel(ByteBuffer byteBuffer, SocketChannel socketChannel) {
        try {
            int total = 0;
            while (byteBuffer.hasRemaining()) {
                int written = socketChannel.write(byteBuffer);
                if (written < 0) {
                    return -1;
                }
                total += written;
            }
            return total;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }
}
