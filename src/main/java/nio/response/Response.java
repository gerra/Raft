package nio.response;

import nio.Transferable;
import nio.request.*;

/**
 * Created by root on 16.06.16.
 */
public abstract class Response implements Transferable {
    public static Response deserializeResponse(String responseString, Request request) {
        responseString = responseString.replace("\r\n", "");
        if (request instanceof PingRequest) {
            return PingResponse.fromString(responseString);
        } else if (request instanceof AcceptNodeRequest) {
            return AcceptNodeResponse.fromString(responseString);
        } else if (request instanceof SetRequest) {
            return SetResponse.fromString(responseString);
        } else if (request instanceof AppendEntriesRequest) {
            return AppendEntriesResponse.fromString(responseString);
        } else if (request instanceof DeleteRequest) {
            return DeleteResponse.fromString(responseString);
        } else if (request instanceof GetRequest) {
            return GetResponse.fromString(responseString);
        } else if (request instanceof RequestVoteRequest) {
            return RequestVoteResponse.fromString(responseString);
        } else {
            throw new IllegalArgumentException("Unknown response");
        }
    }
}
