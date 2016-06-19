package nio.request;

import nio.Transferable;

/**
 * Created by root on 16.06.16.
 */
public abstract class Request implements Transferable {
    public static Request deserializeRequest(String requestString) {
        requestString = requestString.replaceAll("\r\n", "");
        if (requestString.startsWith("node")) {
            return AcceptNodeRequest.fromString(requestString);
        } else if (requestString.startsWith("ping")) {
            return PingRequest.fromString(requestString);
        } else if (requestString.startsWith("set")) {
            return SetRequest.fromString(requestString);
        } else if (requestString.startsWith("appendEntries")) {
            return AppendEntriesRequest.fromString(requestString);
        } else if (requestString.startsWith("delete")) {
            return DeleteRequest.fromString(requestString);
        } else if (requestString.startsWith("get")) {
            return GetRequest.fromString(requestString);
        } else if (requestString.startsWith("requestVote")) {
            return RequestVoteRequest.fromString(requestString);
        }  else {
            return null;
        }
    }
}
