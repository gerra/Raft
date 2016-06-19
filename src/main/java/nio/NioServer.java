package nio;

import nio.model.DeleteCommand;
import nio.model.LogEntry;
import nio.model.SetCommand;
import nio.model.StateMachine;
import nio.request.*;
import nio.response.*;

import java.io.IOException;
import java.net.InetSocketAddress;
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
    private ServerSocketChannel serverSocket;

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
    private Queue<CleintToCandidateRequest> cleintToCandidateRequests = new ArrayDeque<>();

    private static class CleintToCandidateRequest {
        private Request request;
        private SelectionKey client;

        public CleintToCandidateRequest(Request request, SelectionKey client) {
            this.request = request;
            this.client = client;
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
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);

        for (Properties.ServerDescr otherServerDescr : properties.serverDescrs) {
            if (otherServerDescr.id != serverDescr.id) {
                introduceMyselfToOtherServer(otherServerDescr);
            }
        }

        long currentTime = System.currentTimeMillis();
        lastTimeLogReceived = currentTime;
        lastTimeVoteGranted = currentTime;

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
            if (role != Role.CANDIDATE) {
                CleintToCandidateRequest oldRequest = cleintToCandidateRequests.poll();
                if (oldRequest != null) {
                    if (role == Role.LEADER) {
                        executeLeaderRequest(oldRequest.request, oldRequest.client);
                    } else {
                        redirectToLeader(oldRequest.request, oldRequest.client);
                    }
                }
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

                        String inputString = Helper.readFromChannel(channelForRead);
                        boolean validInput = true;
                        if (inputString == null) {
                            Log.e(serverName, "Disconnected: " + selectedKey.hashCode());
                            selectedKey.cancel();
                            channelForRead.close();
                        } else {
                            if (!inputString.isEmpty()) {
                                if (extraInfo.isClient()) {
                                    String[] requestsAsString = inputString.split("\r\n");
                                    for (String requestAsString : requestsAsString) {
                                        Request request = Request.deserializeRequest(requestAsString);
                                        Log.d(serverName, "Request: "
                                                + (request != null ? Helper.gson.toJson(request, request.getClass())
                                                                  : "null"));
                                        if (request instanceof LeaderOnly && role == Role.FOLLOWER) {
                                            redirectToLeader(request, selectedKey);
//                                                extraInfo.addEventWrapper(new EventResponseWrapper(new PingResponse(), null));
                                        } else if (request instanceof LeaderOnly) {
                                            // FIXME oops for client
                                            if (role == Role.FOLLOWER) {
                                                executeLeaderRequest(request, selectedKey);
                                            } else {
                                                cleintToCandidateRequests.add(new CleintToCandidateRequest(
                                                        request,
                                                        selectedKey
                                                ));
                                            }
                                        } else {
                                            if (request instanceof AcceptNodeRequest) {
                                                Log.i(serverName, "acceptNodeRequest");
                                            } else if (request instanceof GetRequest) {
                                                Log.i(serverName, "getRequest");
                                            }
                                            Response response = processRequest(request, selectedKey);
                                            extraInfo.addEventWrapper(new EventResponseWrapper(response));
                                        }
                                    }
                                } else {
                                    String[] responsesAsString = inputString.split("\r\n");
                                    for (String responseAsString : responsesAsString) {
                                        EventRequestWrapper lastRequestWrapper = extraInfo.getResponseQueue().poll();
                                        if (lastRequestWrapper != null) {
                                            Request request = lastRequestWrapper.getRequest();
                                            Response response = Response.deserializeResponse(
                                                    responseAsString,
                                                    request);
                                            Log.d(serverName,
                                                    "Response: " + Helper.gson.toJson(response, response.getClass())
                                                            + "(" + request.getClass().getSimpleName() + ")");
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
                        }
                    } else if (selectedKey.isWritable()) {
                        SocketChannel channelForWrite = (SocketChannel) selectedKey.channel();
                        SelectKeyExtraInfo extraInfo = (SelectKeyExtraInfo) selectedKey.attachment();
                        while (!extraInfo.getOutputQueue().isEmpty()) {
                            EventWrapper curEvent = extraInfo.getOutputQueue().poll();
                            String forSend = null;
                            if (curEvent instanceof EventRequestWrapper) {
                                Request request = ((EventRequestWrapper) curEvent).getRequest();
                                forSend = request.asString();
                                if (request instanceof RequestVoteRequest) {
                                    Log.i(serverName, "Sending RequestVote: " + forSend + " to " + extraInfo.getRemoteServerId());
                                } else if (request instanceof LeaderOnly) {
                                    Log.i(serverName, "Sending leaderOnly request");
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
                                int written = Helper.writeToChannel(Helper.convertStringToByteBuffer(forSend + "\r\n"), channelForWrite);
                                if (written < 0) {
                                    selectedKey.cancel();
                                    channelForWrite.close();
                                    Log.e(serverName, "written < 0 " + forSend);
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
        Set<SelectionKey> leaderSelectionKeys = getSelectionKeyByIdForRequest(currentLeaderId);
        for (SelectionKey leaderSelectionKey : leaderSelectionKeys) {
            Log.i(serverName, "Redirect to leader: "
                    + Helper.gson.toJson(request, request.getClass())
                    + " " + leaderSelectionKey.hashCode());
            leaderSelectionKey.interestOps(SelectionKey.OP_WRITE);
            SelectKeyExtraInfo leaderInfo = (SelectKeyExtraInfo) leaderSelectionKey.attachment();
            leaderInfo.getOutputQueue().add(new EventRequestWrapper(request, new Callback() {
                @Override
                public void onSuccess(Response result) {
                    Log.i(serverName, "Gotten redirected result " + result.asString());
                    extraInfo.getOutputQueue().add(new EventResponseWrapper(result));
                    from.interestOps(SelectionKey.OP_WRITE);
                }
            }));
        }
        if (leaderSelectionKeys.size() > 2) {
            Log.e(serverName, "2 leader connections");
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
        Set<SelectionKey> selectionKeys = getSelectionKeyByIdForRequest(followerId);
        for (SelectionKey selectionKey : selectionKeys) {
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
        Set<SelectionKey> selectionKeys = getSelectionKeyByIdForRequest(followerId);
        for (SelectionKey selectionKey : selectionKeys) {
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
                                    if (callback != null) {
                                        callback.onSuccess(result);
                                    }
                                }
                            }
                    )
            );
        }
        if (selectionKeys.size() == 0) {
            if (callback != null) {
                callback.onError(followerId);
            }
        }
    }

    private void updateLogOnFollowers(Callback<AppendEntriesResponse> callback) {
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
                if (oldLogRunnable.isForAll() && newLogRunnable.isForAll()) {
                    can = false;
                    break;
                }
            }
        }
        return can && updateLogQueue.add(newLogRunnable);
    }

    private Set<SelectionKey> getSelectionKeyByIdForRequest(int id) {
        Set<SelectionKey> res = new HashSet<>();
        int cnt = 0;
        for (SelectionKey selectionKey : selector.keys()) {
            if (selectionKey.isValid()
                    && selectionKey.channel() instanceof SocketChannel
                    && selectionKey.attachment() != null) {
                SelectKeyExtraInfo extraInfo = ((SelectKeyExtraInfo) selectionKey.attachment());
                if (extraInfo.getRemoteServerId() == id
                        && !extraInfo.isClient()
                        && extraInfo.isAccepted()) {
                    cnt++;
                    res.add(selectionKey);
                }
            }
        }
        return res;
    }

    private SelectionKey makeConnectionToOtherServer(Properties.ServerDescr otherServerDescr) {
        try {
            InetSocketAddress otherServerAddress
                    = new InetSocketAddress(otherServerDescr.address.ip, otherServerDescr.address.port);
            SocketChannel otherServerSocket = SocketChannel.open(otherServerAddress);
            otherServerSocket.configureBlocking(false);
            SelectionKey selectionKey = otherServerSocket.register(selector, SelectionKey.OP_WRITE);
            SelectKeyExtraInfo extraInfo = new SelectKeyExtraInfo(false);
            extraInfo.setAccepted(true);
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
            extraInfo.addEventWrapper(new EventRequestWrapper(
                    new AcceptNodeRequest(serverDescr.id),
                    new Callback<AcceptNodeResponse>() {
                        @Override
                        public void onSuccess(AcceptNodeResponse result) {
                            Log.i(serverName, otherServerDescr.id + " accepted us");
                            extraInfo.setAccepted(true);
                        }
                    }));
            extraInfo.setRemoteServerId(otherServerDescr.id);
        } else {
            Log.e(serverName, "Unable to send request to node." + otherServerDescr.id);
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
            if (persistentState.getCurrentTerm() > appendEntries.getTerm()) {
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
        } else {
            return new InvalidRequestResponse();
        }
    }

    private void executeLeaderRequest(Request request, SelectionKey selectionKey) {
        Log.i(serverName, Helper.gson.toJson(request, request.getClass()));
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
                            .add(new EventResponseWrapper(new SetResponse()));
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
                            .add(new EventResponseWrapper(new DeleteResponse(result)));
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
