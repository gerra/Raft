package nio;

import com.google.gson.JsonSyntaxException;
import nio.model.DeleteCommand;
import nio.model.LogEntry;
import nio.model.SetCommand;
import nio.model.StateMachine;
import nio.request.*;
import nio.response.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NioServer {
    private static final int HEART_BEAT_TIME = 50;

    public enum Role {
        LEADER,
        FOLLOWER,
        CANDIDATE
    }

    private static class VolatileState {
        private int commitIndex = 0;
        private int lastApplied = 0;
    }

    private static class LeaderState {
        private Map<Integer, Integer> nextIndex = new ConcurrentHashMap<>();
        private Map<Integer, Integer> matchIndex = new ConcurrentHashMap<>();

        public LeaderState(int id, PersistentState persistentState, Properties properties) {
            for (Properties.ServerDescr serverDescr : properties.serverDescrs) {
                if (id != serverDescr.id) {
                    matchIndex.put(serverDescr.id, 0);
                    nextIndex.put(serverDescr.id, persistentState.getLogSize());
                }
            }
        }
    }

    private Selector selector;
    ServerSocketChannel serverSocket;

    private Properties.ServerDescr serverDescr;
    private Properties properties;
    private final String serverName;
    private volatile Role role;

    private StateMachine stateMachine;
    private PersistentState persistentState;
    private VolatileState volatileState;
    private LeaderState leaderState;

    private long electionTimeOut = 200;
    private long lastTimeLogReceived = -1;
    private long lastTimeVoteGranted = -1;
    private long electionsStartTime = -1;

    private int currentLeaderId = 0;

    private ConcurrentLinkedQueue<Runnable> updateLogQueue = new ConcurrentLinkedQueue<>();

    private String readFromChannel(SocketChannel socketChannel) throws IOException {
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

    private int writeToChannel(ByteBuffer byteBuffer, SocketChannel socketChannel) {
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

    public NioServer(Properties.ServerDescr serverDescr, Properties properties, Role role) throws IOException {
        this.serverDescr = serverDescr;
        this.properties = properties;
        this.role = role;
        serverName = "node." + serverDescr.id;
        stateMachine = new StateMachine();
        persistentState = new PersistentState("dkvs_" + serverDescr.id + ".log");
        volatileState = new VolatileState();
        if (role == Role.LEADER) {
            leaderState = new LeaderState(serverDescr.id, persistentState, properties);
        }

        Log.d(serverName, "Starting...");

        selector = Selector.open();
        serverSocket = ServerSocketChannel.open();
        InetSocketAddress inetSocketAddress = new InetSocketAddress(serverDescr.address.port);

        serverSocket.bind(inetSocketAddress);
        serverSocket.configureBlocking(false);

        for (Properties.ServerDescr otherServerDescr : properties.serverDescrs) {
            if (otherServerDescr.id != serverDescr.id) {
                introduceMyselfToOtherServer(otherServerDescr);
            }
        }

        serverSocket.register(selector, SelectionKey.OP_ACCEPT);

        if (role == Role.LEADER) {
            startTimerTask();
        }

        startLoop();
    }

    private void changeRole(Role newRole) {
        if (role == Role.CANDIDATE) {
            if (newRole == Role.LEADER) {
                Log.i(serverName, "i'm leader");
                role = Role.LEADER;
                startTimerTask();
                leaderState = new LeaderState(serverDescr.id, persistentState, properties);
            } else if (newRole == Role.FOLLOWER) {
                role = Role.FOLLOWER;
            }
        } else if (role == Role.LEADER) {
            if (newRole == Role.FOLLOWER) {
                role = Role.FOLLOWER;
            }
        }
    }

    private void startElections() {
        if (electionsStartTime == -1 && role == Role.CANDIDATE) {
            electionsStartTime = System.currentTimeMillis();
        }
        if (role != Role.CANDIDATE || System.currentTimeMillis() - electionsStartTime > electionTimeOut) {
            Log.i(serverName, "Start voting!");
            electionTimeOut = new Random().nextInt(150) + 150;
            persistentState.voteAndIncrementTerm(serverDescr.id);
            electionsStartTime = System.currentTimeMillis();
            role = Role.CANDIDATE;
            requestVotes();
        }
    }

    private void checkConvertToCandidate() {
        long currentTime = System.currentTimeMillis();
        if (lastTimeLogReceived == -1) {
            lastTimeLogReceived = currentTime;
        }
        if (lastTimeVoteGranted == -1) {
            lastTimeVoteGranted = currentTime;
        }
        if (currentTime - lastTimeLogReceived > electionTimeOut
                && currentTime - lastTimeVoteGranted > electionTimeOut) {
            startElections();
        }
    }

    private void startLoop() {
        while (true) {
            Runnable runnable = updateLogQueue.poll();
            if (runnable != null) {
                runnable.run();
            }
            if (role != Role.LEADER) {
                checkConvertToCandidate(); // for followers
            }

            try {
                selector.select(properties.timeout);
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                for (SelectionKey selectedKey : selectedKeys) {
                    if (selectedKey.isAcceptable()) {
                        SocketChannel clientSocket = serverSocket.accept();
                        if (clientSocket != null) {
                            clientSocket.configureBlocking(false);
                            SelectionKey addedKey = clientSocket.register(selector, SelectionKey.OP_READ);
                            addedKey.attach(new SelectKeyExtraInfo(true));
                            Log.d(serverName, "Accepted: " + clientSocket.getRemoteAddress());
                        }
                    } else if (selectedKey.isReadable()) {
                        SocketChannel channelForRead = (SocketChannel) selectedKey.channel();
                        SelectKeyExtraInfo extraInfo = (SelectKeyExtraInfo) selectedKey.attachment();

                        String inputString = readFromChannel(channelForRead);
                        if (inputString == null) {
                            Log.d(serverName, "Disconnected: " + channelForRead.getRemoteAddress());
                            selectedKey.cancel();
                            channelForRead.close();
                        } else {
                            try {
                                if (!inputString.isEmpty()) {
                                    if (extraInfo.isClient()) {
                                        String[] requestsAsString = inputString.split("\r\n");
                                        for (String requestAsString : requestsAsString) {
                                            Request request = deserializeRequest(requestAsString);
                                            if (request == null) {
                                                continue;
                                            }
                                            if (request instanceof LeaderOnly && role == Role.FOLLOWER) {
                                                redirectToLeader(request, selectedKey);
//                                                extraInfo.addEventWrapper(new EventResponseWrapper(new PingResponse(), null));
                                            } else if (request instanceof LeaderOnly) {
                                                // FIXME oops for client
                                                executeLeaderRequest(request, selectedKey);
                                            } else {
                                                Response responseForSend = processRequest(request, selectedKey);
                                                extraInfo.addEventWrapper(new EventResponseWrapper(responseForSend, null));
                                            }
                                        }
                                    } else {
                                        String[] responsesAsString = inputString.split("\r\n");
                                        for (String responseAsString : responsesAsString) {
                                            EventRequestWrapper lastRequestWrapper = extraInfo.getResponseQueue().poll();
                                            if (lastRequestWrapper != null) {
                                                Response response = deserializeResponse(
                                                        responseAsString,
                                                        lastRequestWrapper.getRequest());
                                                if (lastRequestWrapper.getCallback() != null) {
                                                    lastRequestWrapper.getCallback().onSuccess(response);
                                                }
                                            } else {
                                                Log.e(serverName, "Response without request? " + responseAsString);
                                            }
                                        }
                                    }
                                    selectedKey.interestOps(SelectionKey.OP_WRITE);
                                }
                            } catch (JsonSyntaxException e) {
                                e.printStackTrace();
                                Log.e(serverName, inputString);
                            }
                        }
                    } else if (selectedKey.isWritable()) {
                        SocketChannel channelForWrite = (SocketChannel) selectedKey.channel();
                        SelectKeyExtraInfo extraInfo = (SelectKeyExtraInfo) selectedKey.attachment();
//                        if (extraInfo.getLastEventWrapper() != null) {
//                            if (!extraInfo.getOutputQueue().isEmpty()) {
//                                Log.e(serverName, "we didn't process previous request "
//                                        + Helper.gson.toJson(
//                                            ((EventRequestWrapper) extraInfo.getLastEventWrapper()).getRequest()));
//                            }
//                            selectedKey.interestOps(SelectionKey.OP_READ);
//                            continue;
//                        }
                        while (!extraInfo.getOutputQueue().isEmpty()) {
                            EventWrapper curEvent = extraInfo.getOutputQueue().poll();
                            String forSend = null;
                            if (curEvent instanceof EventRequestWrapper) {
                                Request request = ((EventRequestWrapper) curEvent).getRequest();
                                forSend = request.asString();
                                if (request instanceof RequestVoteRequest) {
                                    Log.i(serverName, "Sending RequestVote: " + forSend + " to " + extraInfo.getRemoteServerId());
                                }
                            } else if (curEvent instanceof EventResponseWrapper) {
                                Response response = ((EventResponseWrapper) curEvent).getResponse();
                                forSend = response.asString();
                                if (response instanceof RequestVoteResponse) {
                                    Log.i(serverName, "Sending response for RequestVote: " + forSend + " to " + extraInfo.getRemoteServerId());
                                }
                            }
                            if (forSend != null) {
                                Log.d(serverName, "Request or Response: " + forSend);
                                int written = writeToChannel(Helper.convertStringToByteBuffer(forSend + "\r\n"), channelForWrite);
                                if (written < 0) {
                                    selectedKey.cancel();
                                    channelForWrite.close();
                                } else {
                                    if (curEvent instanceof EventRequestWrapper) {
                                        extraInfo.getResponseQueue().add((EventRequestWrapper) curEvent);
                                    }
                                    selectedKey.interestOps(SelectionKey.OP_READ);
                                }
                            }
                        }
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void redirectToLeader(Request request, SelectionKey from) {
        SelectKeyExtraInfo extraInfo = (SelectKeyExtraInfo) from.attachment();
        SelectionKey leaderSelectionKey = getSelectionKeyByIdForWrite(currentLeaderId);
        if (leaderSelectionKey != null) {
            leaderSelectionKey.interestOps(SelectionKey.OP_WRITE);
            SelectKeyExtraInfo leaderInfo = (SelectKeyExtraInfo) leaderSelectionKey.attachment();
            leaderInfo.getOutputQueue().add(new EventRequestWrapper(request, new Callback() {
                @Override
                public void onSuccess(Response result) {
                    extraInfo.getOutputQueue().add(new EventResponseWrapper(result, null));
                    from.interestOps(SelectionKey.OP_WRITE);
                }
            }));
        }
    }

    private void startTimerTask() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (role != Role.LEADER) {
                    updateLogQueue.clear();
                    cancel();
                } else {
                    addUpdateLogRunnable(new UpdateLogRunnable(true) {
                        @Override
                        public void run() {
                            updateLogOnFollowers(null);
                        }
                    });
                }
            }
        }, 0, HEART_BEAT_TIME);
    }

    private boolean checkCommitIndex(int commitIndex) {
        int cnt = 0;
        for (Integer matchIndex : leaderState.matchIndex.values()) {
            if (matchIndex >= commitIndex) {
                cnt++;
            }
        }
        return cnt >= properties.serverDescrs.size() / 2;
    }

    private void applyLogCommands() {
        for (int i = volatileState.lastApplied; i < volatileState.commitIndex; i++) {
            persistentState.getLogEntry(i).getCommand().apply(stateMachine);
        }
        volatileState.lastApplied = volatileState.commitIndex;
    }

    private void updateCommitIndexAndApplyToStateMachine() {
        int commitIndex = volatileState.commitIndex + 1;
        while (checkCommitIndex(commitIndex)) {
            commitIndex++;
        }
        commitIndex--;
        volatileState.commitIndex = commitIndex;
        if (volatileState.lastApplied < volatileState.commitIndex) {
            applyLogCommands();
        }
        Log.d(serverName, "updateCommitIndexAndApplyToStateMachine(): " + commitIndex);
    }

    private boolean checkTermAndConvertToFollower(int term) {
        if (term > persistentState.getCurrentTerm()) {
            persistentState.updateCurrentTerm(term);
            changeRole(Role.FOLLOWER);
            return false;
        }
        return true;
    }

    private void requestVoteById(int followerId, Callback<RequestVoteResponse> callback) {
        SelectionKey selectionKey = getSelectionKeyByIdForWrite(followerId);
        if (selectionKey == null) {
            return;
        }
        SelectKeyExtraInfo extraInfo = ((SelectKeyExtraInfo) selectionKey.attachment());
        int logSize = persistentState.getLogSize();
        RequestVoteRequest request = new RequestVoteRequest(
                persistentState.getCurrentTerm(),
                serverDescr.id,
                logSize - 1,
                logSize == 0 ? 0 : persistentState.getLogEntry(logSize - 1).getTerm()
        );
        extraInfo.getOutputQueue().add(new EventRequestWrapper(
                request,
                new Callback<RequestVoteResponse>() {
                    @Override
                    public void onSuccess(RequestVoteResponse result) {
                        callback.onSuccess(result);
                        checkTermAndConvertToFollower(result.getTerm());
                    }
                }
        ));
    }

    private void requestVotes() {
        int needToGrant = properties.serverDescrs.size() / 2;
        for (Properties.ServerDescr otherServer : properties.serverDescrs) {
            if (otherServer.id != this.serverDescr.id) {
                requestVoteById(otherServer.id, new Callback<RequestVoteResponse>() {
                    int granted = 0;

                    @Override
                    public void onSuccess(RequestVoteResponse result) {
                        Log.i(serverName, result.isVoteGranted() + " from " + otherServer.id);

                        if (result.isVoteGranted()) {
                            granted++;
                        }
                        if (granted >= needToGrant) {
                            changeRole(Role.LEADER);
                        }
                    }
                });
            }
        }
    }

    private void updateLogOnFollower(int followerId, Callback<AppendEntriesResponse> callback) {
        if (callback == null) {
            callback = new AppendEntriesResponseCallback(this);
        }
        SelectionKey selectionKey = getSelectionKeyByIdForWrite(followerId);
        if (selectionKey != null) {
            SelectKeyExtraInfo extraInfo = ((SelectKeyExtraInfo) selectionKey.attachment());
            int nextIndex = leaderState.nextIndex.get(followerId);
            int prevLogIndex = nextIndex - 1;

            AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(
                    persistentState.getCurrentTerm(),
                    serverDescr.id,
                    prevLogIndex < 0 ? -1 : prevLogIndex,
                    prevLogIndex < 0 ? 0 : persistentState.getLogEntry(prevLogIndex).getTerm(),
                    persistentState.getLogSuffix(nextIndex),
                    volatileState.commitIndex
            );

            final Callback<AppendEntriesResponse> finalCallback = callback;
            final int oldLogSize = persistentState.getLogSize();
            extraInfo.getOutputQueue().add(
                    new EventRequestWrapper(
                            appendEntriesRequest,
                            new AppendEntriesResponseCallback(this) {
                                @Override
                                public void onSuccess(AppendEntriesResponse result) {
                                    if (result.isSuccess()) {
                                        leaderState.matchIndex.put(followerId, oldLogSize);
                                        leaderState.nextIndex.put(followerId, oldLogSize);
                                        updateCommitIndexAndApplyToStateMachine();
                                    } else {
                                        if (checkTermAndConvertToFollower(result.getTerm())) {
                                            int newNextIndex = appendEntriesRequest.getPrevLogIndex();
                                            leaderState.nextIndex.put(followerId, newNextIndex);
                                            Callback thisCallback = this;
                                            updateLogQueue.add(new UpdateLogRunnable(followerId) {
                                                @Override
                                                public void run() {
                                                    updateLogOnFollower(followerId, thisCallback);
                                                }
                                            });
                                        }
                                    }
                                    finalCallback.onSuccess(result);
                                }

                                @Override
                                public void onError(Object object) {
                                    finalCallback.onError(object);
                                }
                            }
                    )
            );
        } else {
            callback.onError(followerId);
        }
    }

    private void updateLogOnFollowers(Callback<AppendEntriesResponse> callback) {
        if (callback == null) {
            callback = new AppendEntriesResponseCallback(this);
        }
        for (Properties.ServerDescr follower : properties.serverDescrs) {
            if (follower.id != this.serverDescr.id) {
                updateLogOnFollower(follower.id, callback);
            }
        }
    }

    private boolean addUpdateLogRunnable(UpdateLogRunnable newLogRunnable) {
        boolean can = true;
        for (Runnable runnable : updateLogQueue) {
            if (runnable instanceof UpdateLogRunnable) {
                UpdateLogRunnable oldLogRunnable = (UpdateLogRunnable) runnable;
                if (oldLogRunnable.isForAll() && oldLogRunnable.isForAll()) {
                    can = false;
                    break;
                }
            }
        }
        return can && updateLogQueue.add(newLogRunnable);
    }

    private SelectionKey getSelectionKeyByIdForWrite(int id) {
        for (SelectionKey selectionKey : selector.keys()) {
            if (selectionKey.isValid() && selectionKey.channel() instanceof SocketChannel && selectionKey.attachment() != null) {
                SelectKeyExtraInfo extraInfo = ((SelectKeyExtraInfo) selectionKey.attachment());
                if (extraInfo.getRemoteServerId() == id
                        && !extraInfo.isClient()
                        ) {
                    return selectionKey;
                }
            }
        }
        return null;
    }

    private SelectionKey makeConnectionToOtherServer(Properties.ServerDescr otherServerDescr) {
        try {
            InetSocketAddress otherServerAddress
                    = new InetSocketAddress(otherServerDescr.address.ip, otherServerDescr.address.port);
            SocketChannel otherServerSocket = SocketChannel.open(otherServerAddress);
            otherServerSocket.configureBlocking(false);
            SelectionKey selectionKey = otherServerSocket.register(selector, SelectionKey.OP_WRITE);
            SelectKeyExtraInfo extraInfo = new SelectKeyExtraInfo(false);
            extraInfo.setRemoteServerId(otherServerDescr.id);
            selectionKey.attach(extraInfo);
            return selectionKey;
        } catch (IOException e) {
            Log.e(serverName, "Unable to connect to node." + otherServerDescr.id + "(" + e.getMessage() + ")");
        }
        return null;
    }

    private void introduceMyselfToOtherServer(Properties.ServerDescr otherServerDescr) {
        if (otherServerDescr.id == serverDescr.id) {
            return;
        }
        SelectionKey selectionKey = makeConnectionToOtherServer(otherServerDescr);
        if (selectionKey != null) {
            SelectKeyExtraInfo extraInfo = (SelectKeyExtraInfo) selectionKey.attachment();
            extraInfo.addEventWrapper(
                    new EventRequestWrapper(new AcceptNodeRequest(serverDescr.id), new Callback<AcceptNodeResponse>() {
                        @Override
                        public void onSuccess(AcceptNodeResponse result) {
                            extraInfo.setAccepted(true);
                        }
                    })
            );
            extraInfo.setRemoteServerId(otherServerDescr.id);
        } else {
            Log.e(serverName, "Unable to send request to node." + otherServerDescr.id);
        }
    }

    private Request deserializeRequest(String requestString) {
        requestString = requestString.replaceAll("\r\n", "");
        Log.d(serverName, "Request: " + requestString);
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

    private Response deserializeResponse(String responseString, Request request) {
        responseString = responseString.replace("\r\n", "");
        Log.d(serverName, "Response: " + responseString + "(" + request.getClass().getSimpleName() + ")");
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

    private Response processRequest(Request request, SelectionKey selectionKey) {
        if (request instanceof AcceptNodeRequest) {
            int nodeId = ((AcceptNodeRequest) request).getId();
            makeConnectionToOtherServer(properties.getServerDescrById(nodeId));
            ((SelectKeyExtraInfo) selectionKey.attachment()).setRemoteServerId(nodeId);
            return new AcceptNodeResponse();
        } else if (request instanceof PingRequest) {
            return new PingResponse();
        } else if (request instanceof AppendEntriesRequest) {
            lastTimeLogReceived = System.currentTimeMillis();
            if (role == Role.CANDIDATE) {
                changeRole(Role.FOLLOWER);
            }

            AppendEntriesRequest appendEntries = (AppendEntriesRequest) request;
            if (currentLeaderId != appendEntries.getLeaderId()) {
                currentLeaderId = appendEntries.getLeaderId();
                Log.i(serverName, "Current leader is " + currentLeaderId);
            }
            if (persistentState.getCurrentTerm() < appendEntries.getTerm()) {
                persistentState.updateCurrentTerm(appendEntries.getTerm());
            }

            int logSize = persistentState.getLogSize();
            int prevLogIndex = appendEntries.getPrevLogIndex();
            int prevLogTerm = appendEntries.getPrevLogTerm();
            AppendEntriesResponse response;
            if (prevLogIndex != -1 && persistentState.
                    getCurrentTerm() > appendEntries.getTerm()) {
                response = new AppendEntriesResponse(persistentState.getCurrentTerm(), false);
            } else if (prevLogIndex != -1 && (logSize <= prevLogIndex || persistentState.getLogEntry(prevLogIndex).getTerm() != prevLogTerm)) {
                response = new AppendEntriesResponse(persistentState.getCurrentTerm(), false);
            } else {
                int i;
                int newEntriesSize = appendEntries.getEntries().size();
                for (i = 0; i < newEntriesSize; i++) {
                    LogEntry newEntry = appendEntries.getEntries().get(i);
                    int newEntryIndex = prevLogIndex + i + 1;
                    if (newEntryIndex >= logSize) {
                        break;
                    }
                    LogEntry currentEntry = persistentState.getLogEntry(newEntryIndex);
                    if (newEntry.getTerm() != currentEntry.getTerm()) {
                        persistentState.clearLogSuffix(newEntryIndex);
                        break;
                    }
                }
                if (i != newEntriesSize) {
                    persistentState.addLogEntries(appendEntries.getEntries().subList(i, newEntriesSize));
                }
                if (appendEntries.getLeaderCommit() > volatileState.commitIndex) {
                    volatileState.commitIndex = Math.min(appendEntries.getLeaderCommit(), persistentState.getLogSize());
                    applyLogCommands();
                }
                response = new AppendEntriesResponse(persistentState.getCurrentTerm(), true);
            }
            return response;
        } else if (request instanceof GetRequest) {
            String key = ((GetRequest) request).getKey();
            String value = stateMachine.get(key);
            if (value == null) {
                return new GetResponse(true);
            }
            return new GetResponse(key, value);
        } else if (request instanceof RequestVoteRequest) {
            RequestVoteRequest requestVote = (RequestVoteRequest) request;
            Log.i(serverName, "RequestVote: from " + requestVote.getCandidateId());
            if (requestVote.getTerm() > persistentState.getCurrentTerm()) {
                persistentState.updateCurrentTerm(requestVote.getTerm());
            }
            RequestVoteResponse response = null;
            if (persistentState.getCurrentTerm() > requestVote.getTerm()) {
                response = new RequestVoteResponse(persistentState.getCurrentTerm(), false);
            } else {
                Integer votedFor = persistentState.getVotedFor();
                if (votedFor == null || votedFor == requestVote.getCandidateId()) {
                    int lastIndex = persistentState.getLogSize() - 1;
                    int lastTerm = lastIndex >= 0 ? persistentState.getLogEntry(lastIndex).getTerm() : 0;
                    if (requestVote.getLastLogTerm() > lastTerm
                            || requestVote.getLastLogTerm() == lastTerm && requestVote.getLastLogIndex() >= lastIndex) {
                        lastTimeVoteGranted = System.currentTimeMillis();
                        response = new RequestVoteResponse(persistentState.getCurrentTerm(), true);
                        persistentState.setVotedFor(requestVote.getCandidateId());
                    }
                }
            }
            if (response == null) {
                response = new RequestVoteResponse(persistentState.getCurrentTerm(), false);
            }
            Log.i(serverName, "RequestVote: " + response.isVoteGranted() + " for " + requestVote.getCandidateId());
            return response;
        }
        return null;
    }

    private void executeLeaderRequest(Request request, SelectionKey selectionKey) {
        // candidate
        int needForResponse = properties.serverDescrs.size() / 2;
        LogEntry logEntry;
        Runnable onCommitEntry;
        if (request instanceof SetRequest) {
            SetRequest setRequest = (SetRequest) request;
            logEntry = new LogEntry(
                    persistentState.getCurrentTerm(),
                    new SetCommand(setRequest.getKey(), setRequest.getValue())
            );
            onCommitEntry = () -> {
                if (selectionKey.isValid()) {
                    ((SelectKeyExtraInfo) selectionKey.attachment()).getOutputQueue()
                            .add(new EventResponseWrapper(new SetResponse(), null));
                    selectionKey.interestOps(SelectionKey.OP_WRITE);
                }
            };
        } else if (request instanceof DeleteRequest) {
            DeleteRequest deleteRequest = (DeleteRequest) request;
            logEntry = new LogEntry(
                    persistentState.getCurrentTerm(),
                    new DeleteCommand(deleteRequest.getKey())
            );
            onCommitEntry = () -> {
                if (selectionKey.isValid()) {
                    boolean result = logEntry.getCommand().getResult();
                    ((SelectKeyExtraInfo) selectionKey.attachment()).getOutputQueue()
                            .add(new EventResponseWrapper(new DeleteResponse(result), null));
                }
                selectionKey.interestOps(SelectionKey.OP_WRITE);
            };
        } else {
            return;
        }
        persistentState.addLogEntry(logEntry);
        if (needForResponse == 0) {
            updateCommitIndexAndApplyToStateMachine();
            onCommitEntry.run();
            return;
        }
        updateLogQueue.add(new UpdateLogRunnable(true) {
            @Override
            public void run() {
                updateLogOnFollowers(new AppendEntriesResponseCallback(NioServer.this) {
                    @Override
                    public void onSuccess(AppendEntriesResponse result) {
                        super.onSuccess(result);
                        if (this.successCount == needForResponse) {
                            Log.i(serverName, "SetRequest: information on the half of servers");
                            onCommitEntry.run();
                        }
                    }

                    @Override
                    public void onError(Object object) {
                        super.onError(object);
                        if (object instanceof Integer) {
                            int failedFollowerId = ((int) object);
                            AppendEntriesResponseCallback callback = this;
                            updateLogQueue.add(new UpdateLogRunnable(failedFollowerId) {
                                @Override
                                public void run() {
                                    updateLogOnFollower(failedFollowerId, callback);
                                }
                            });
                        }
                    }
                });
            }
        });
    }

    private static class AppendEntriesResponseCallback extends Callback<AppendEntriesResponse> {
        private NioServer nioServer;
        protected int successCount = 0;

        public AppendEntriesResponseCallback(NioServer nioServer) {
            this.nioServer = nioServer;
        }

        @Override
        public void onSuccess(AppendEntriesResponse result) {
            Log.d(nioServer.serverName, "AppendEntries onSuccess " + result.asString());
            if (result.isSuccess()) {
                successCount++;
            }
        }
    }
}
