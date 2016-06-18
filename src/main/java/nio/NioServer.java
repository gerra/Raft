package nio;

import nio.model.DeleteCommand;
import nio.model.LogEntry;
import nio.model.SetCommand;
import nio.model.StateMachine;
import nio.request.*;
import nio.response.*;
import raft.Utils;

import java.io.*;
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
    private static final int HEART_BEAT_TIME = 500;

    public enum Role {
        LEADER,
        FOLLOWER,
        CANDIDATE
    }

    private static class PersistentState {
        private transient String logFileName;
        private transient String log2FileName;
        private int currentTerm = 1;
        private Integer votedFor;
        private List<LogEntry> log = new ArrayList<>();

        public void updateCurrentTerm(int newTerm) {
            if (newTerm > currentTerm) {
                currentTerm = newTerm;
                votedFor = null;
            }
        }

        public PersistentState(String logFileName) throws IOException {
            this.logFileName = logFileName;
            log2FileName = logFileName + "e";
            File logFile = new File(logFileName);
            File log2File = new File(log2FileName);
            if (!logFile.exists()) {
                logFile.createNewFile();
            }
            if (!log2File.exists()) {
                log2File.createNewFile();
            }
            try (BufferedReader logReader = new BufferedReader(new FileReader(logFile))) {
                String s = logReader.readLine();
                if (s != null && !s.isEmpty()) {
                    log.add(Helper.gson.fromJson(s, LogEntry.class));
                }
            }
            try (BufferedReader log2Reader = new BufferedReader(new FileReader(log2File))) {
                String termS = log2Reader.readLine();
                String votedS = log2Reader.readLine();
                if (termS != null) {
                    currentTerm = Integer.parseInt(termS);
                    if (votedS != null) {
                        votedFor = Integer.parseInt(votedS);
                    }
                }
            }

//            log.add(new LogEntry(1, new DeleteCommand("x")));
//            saveToStorage();
        }

        public void saveToStorage() {
            try {
                try (BufferedWriter log2Writer = new BufferedWriter(new FileWriter(log2FileName))) {
                    log2Writer.write(String.valueOf(currentTerm) + "\n");
                    if (votedFor != null) {
                        log2Writer.write(String.valueOf(votedFor) + "\n");
                    }
                }
                try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFileName))) {
                    for (LogEntry logEntry : log) {
                        logWriter.write(Helper.gson.toJson(logEntry, LogEntry.class) + "\n");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
                    nextIndex.put(serverDescr.id, persistentState.log.size());
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

    private long electionTimeOut = 2_000;
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

    private int sendMessage(ByteBuffer byteBuffer, SocketChannel socketChannel) {
        try {
            return socketChannel.write(byteBuffer);
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
        }
    }

    private void startElections() {
        if (electionsStartTime == -1 && role == Role.CANDIDATE) {
            electionsStartTime = System.currentTimeMillis();
        }
        if (role != Role.CANDIDATE || System.currentTimeMillis() - electionsStartTime > electionTimeOut) {
            Log.i(serverName, "Start voting!");
            persistentState.votedFor = serverDescr.id;
            persistentState.currentTerm++;
            requestVotes();
            role = Role.CANDIDATE;
            electionsStartTime = System.currentTimeMillis();
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
                            if (!inputString.isEmpty()) {
                                if (extraInfo.isClient()) {
                                    Request request = deserializeRequest(inputString);
                                    if (request == null) {
                                        continue;
                                    }
                                    if (request instanceof LeaderOnly && role == Role.FOLLOWER) {
                                        redirectToLeader(request, selectedKey);
                                    } else if (request instanceof LeaderOnly) {
                                        executeLeaderRequest(request, selectedKey);
                                    } else {
                                        Response responseForSend = processRequest(request, selectedKey);
                                        extraInfo.addEventWrapper(new EventResponseWrapper(responseForSend, null));
                                    }
                                } else {
                                    Response response = deserializeResponse(
                                            inputString,
                                            ((EventRequestWrapper) extraInfo.getLastEventWrapper()).getRequest());
                                    EventWrapper lastEventWrapper = extraInfo.getLastEventWrapper();
                                    if (lastEventWrapper != null && lastEventWrapper.getCallback() != null) {
                                        lastEventWrapper.getCallback().onSuccess(response);
                                    }
                                    extraInfo.setLastEventWrapper(null);
                                }
                                selectedKey.interestOps(SelectionKey.OP_WRITE);
                            }
                        }
                    } else if (selectedKey.isWritable()) {
                        SocketChannel channelForWrite = (SocketChannel) selectedKey.channel();
                        SelectKeyExtraInfo extraInfo = (SelectKeyExtraInfo) selectedKey.attachment();
                        if (extraInfo.getLastEventWrapper() != null) {
                            if (!extraInfo.getQueue().isEmpty()) {
                                Log.e(serverName, "we didn't process previous request, continue, very strange behavior");
                            }
                            selectedKey.interestOps(SelectionKey.OP_READ);
                            continue;
                        }
                        EventWrapper curEvent = extraInfo.getQueue().poll();
                        if (curEvent != null) {
                            String forSend = null;
                            if (curEvent instanceof EventRequestWrapper) {
                                forSend = ((EventRequestWrapper) curEvent).getRequest().asString();
                            } else if (curEvent instanceof EventResponseWrapper) {
                                forSend = ((EventResponseWrapper) curEvent).getResponse().asString();
                            }
                            if (forSend != null) {
                                Log.d(serverName, "Request or Response: " + forSend + "(sk " + selectedKey.hashCode() + ")");
                                int written = sendMessage(Utils.convertStringToByteBuffer(forSend + "\r\n"), channelForWrite);
                                if (written < 0) {
                                    selectedKey.cancel();
                                    channelForWrite.close();
                                } else {
                                    if (!extraInfo.isClient()) {
                                        extraInfo.setLastEventWrapper(curEvent);
                                    } else {
                                        extraInfo.setLastEventWrapper(null);
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
            leaderInfo.getQueue().add(new EventRequestWrapper(request, new Callback() {
                @Override
                public void onSuccess(Response result) {
                    extraInfo.getQueue().add(new EventResponseWrapper(result, null));
                    from.interestOps(SelectionKey.OP_WRITE);
                }
            }));
        }
        from.interestOps(SelectionKey.OP_WRITE);
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
                    updateLogQueue.add(new UpdateLogRunnable(true) {
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
            persistentState.log.get(i).getCommand().apply(stateMachine);
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

    private void requestVoteById(int followerId, Callback<RequestVoteResponse> callback) {
        SelectionKey selectionKey = getSelectionKeyByIdForWrite(followerId);
        if (selectionKey == null) {
//            Log.e(serverName, " wtf");
            return;
        }
        SelectKeyExtraInfo extraInfo = ((SelectKeyExtraInfo) selectionKey.attachment());

        int logSize = persistentState.log.size();
        RequestVoteRequest request = new RequestVoteRequest(
                persistentState.currentTerm,
                serverDescr.id,
                logSize,
                logSize == 0 ? 0 : persistentState.log.get(logSize - 1).getTerm()
        );
        extraInfo.getQueue().add(new EventRequestWrapper(
                request,
                callback
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
                        Log.i(serverName, result.isVoteGranted() + " from " + serverDescr.id);

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
                    persistentState.currentTerm,
                    serverDescr.id,
                    prevLogIndex < 0 ? -1 : prevLogIndex,
                    prevLogIndex < 0 ? 0 : persistentState.log.get(prevLogIndex).getTerm(),
                    new ArrayList<>(persistentState.log.subList(nextIndex, persistentState.log.size())),
                    volatileState.commitIndex
            );

            final Callback<AppendEntriesResponse> finalCallback = callback;
            final int oldLogSize = persistentState.log.size();
            extraInfo.getQueue().add(
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
                if (oldLogRunnable.isForAll()) {
                    can = false;
                    break;
                }
                if (newLogRunnable.getUpdateFollowerId() == oldLogRunnable.getUpdateFollowerId()) {
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
                if (extraInfo.getRemoteServerId() == id && !extraInfo.isClient()) {
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
            ((SelectKeyExtraInfo) selectionKey.attachment())
                    .addEventWrapper(new EventRequestWrapper(new AcceptNodeRequest(serverDescr.id), null));
            ((SelectKeyExtraInfo) selectionKey.attachment())
                    .setRemoteServerId(otherServerDescr.id);
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
            int currentTerm = persistentState.currentTerm;
            List<LogEntry> log = persistentState.log;
            int prevLogIndex = appendEntries.getPrevLogIndex();
            int prevLogTerm = appendEntries.getPrevLogTerm();
            AppendEntriesResponse response;
            if (prevLogIndex != -1 && currentTerm > appendEntries.getTerm()) {
                response = new AppendEntriesResponse(currentTerm, false);
            } else if (prevLogIndex != -1 && (log.size() <= prevLogIndex || log.get(prevLogIndex).getTerm() != prevLogTerm)) {
                response = new AppendEntriesResponse(currentTerm, false);
            } else {
                int i;
                int newEntriesSize = appendEntries.getEntries().size();
                for (i = 0; i < newEntriesSize; i++) {
                    LogEntry newEntry = appendEntries.getEntries().get(i);
                    int newEntryIndex = prevLogIndex + i + 1;
                    if (newEntryIndex >= log.size()) {
                        break;
                    }
                    LogEntry currentEntry = log.get(newEntryIndex);
                    if (newEntry.getTerm() != currentEntry.getTerm()) {
                        log.subList(newEntryIndex, log.size()).clear();
                        break;
                    }
                }
                if (i != newEntriesSize) {
                    log.addAll(appendEntries.getEntries().subList(i, newEntriesSize));
                }
                if (appendEntries.getLeaderCommit() > volatileState.commitIndex) {
                    volatileState.commitIndex = Math.min(appendEntries.getLeaderCommit(), log.size());
                    applyLogCommands();
                }
                if (currentTerm < appendEntries.getTerm()) {
                    persistentState.updateCurrentTerm(appendEntries.getTerm());
                }
                response = new AppendEntriesResponse(persistentState.currentTerm, true);
            }
            persistentState.saveToStorage();
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
            if (requestVote.getTerm() > persistentState.currentTerm) {
                persistentState.updateCurrentTerm(requestVote.getTerm());
                persistentState.saveToStorage();
            }
            RequestVoteResponse response = null;
            if (persistentState.currentTerm > requestVote.getTerm()) {
                response = new RequestVoteResponse(persistentState.currentTerm, false);
            } else {
                Integer votedFor = persistentState.votedFor;
                if (votedFor == null || requestVote.getCandidateId() == serverDescr.id) {
                    int lastIndex = persistentState.log.size();
                    int lastTerm = lastIndex != 0 ? persistentState.log.get(lastIndex - 1).getTerm() : -1;
                    if (requestVote.getLastLogTerm() > lastTerm
                            || requestVote.getLastLogTerm() == lastTerm && requestVote.getLastLogIndex() >= lastIndex) {
                        lastTimeVoteGranted = System.currentTimeMillis();
                        response = new RequestVoteResponse(persistentState.currentTerm, true);
                        persistentState.votedFor = requestVote.getCandidateId();
                        persistentState.saveToStorage();
                    }
                }
            }
            if (response == null) {
                response = new RequestVoteResponse(persistentState.currentTerm, false);
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
                    persistentState.currentTerm,
                    new SetCommand(setRequest.getKey(), setRequest.getValue())
            );
            onCommitEntry = () -> {
                Log.d(serverName, "SetRequest: information on the half of servers, build response");
                if (selectionKey.isValid()) {
                    ((SelectKeyExtraInfo) selectionKey.attachment()).getQueue()
                            .add(new EventResponseWrapper(new SetResponse(), null));
                }
                selectionKey.interestOps(SelectionKey.OP_WRITE);
            };
        } else if (request instanceof DeleteRequest) {
            DeleteRequest deleteRequest = (DeleteRequest) request;
            logEntry = new LogEntry(
                    persistentState.currentTerm,
                    new DeleteCommand(deleteRequest.getKey())
            );
            onCommitEntry = () -> {
                Log.d(serverName, "DeleteRequest: information on the half of servers, build response");
                if (selectionKey.isValid()) {
                    boolean result = logEntry.getCommand().getResult();
                    ((SelectKeyExtraInfo) selectionKey.attachment()).getQueue()
                            .add(new EventResponseWrapper(new DeleteResponse(result), null));
                }
                selectionKey.interestOps(SelectionKey.OP_WRITE);
            };
        } else {
            return;
        }
        persistentState.log.add(logEntry);
        if (needForResponse == 0) {
            updateCommitIndexAndApplyToStateMachine();
            onCommitEntry.run();
            return;
        }
        persistentState.saveToStorage();
        updateLogQueue.add(new UpdateLogRunnable(true) {
            @Override
            public void run() {
                updateLogOnFollowers(new AppendEntriesResponseCallback(NioServer.this) {
                    @Override
                    public void onSuccess(AppendEntriesResponse result) {
                        super.onSuccess(result);
                        if (this.successCount == needForResponse) {
                            onCommitEntry.run();
                        } else if (role != Role.LEADER) {
                            redirectToLeader(request, selectionKey);
                        }
                    }

                    @Override
                    public void onError(Object object) {
                        super.onError(object);
                        if (object instanceof Integer) {
                            int failedFollowerId = ((int) object);
                            AppendEntriesResponseCallback callback = this;
                            addUpdateLogRunnable(new UpdateLogRunnable(failedFollowerId) {
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
