package raft.server;

import com.sun.istack.internal.NotNull;
import nio.Log;
import nio.Properties;
import raft.LogEntry;
import raft.server.request.*;
import raft.statemachine.DeleteCommand;
import raft.statemachine.MyStateMachine;
import raft.statemachine.SetCommand;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

public class Server implements AutoCloseable {
    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private static class PersistentState {
        private transient String logFileName;
        private transient String log2FileName;
        private int currentTerm = 1;
        private Integer votedFor;
        private List<LogEntry> log = new ArrayList<>();

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
                int c = logReader.read();
                while (c != -1) {
                    if (c != '\n') {
                        try {
                            log.add(LogEntry.read(logReader, c));
                        } catch (IOException ignored) {}
                    }
                    c = logReader.read();
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
                        logWriter.write(logEntry.toString() + "\n");
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

    // General fields
    /**
     * Id of server
     */
    private Integer id;
    /**
     * Socket for listening of new connections
     */
    private ServerSocket socket;
    /**
     *
     */
    private Properties properties;

    private Map<Integer, Address> addressMap = new HashMap<>();

    private Map<Address, ConcurrentLinkedQueue<Request>> outputEventQueues = new HashMap<>();
    private Map<Address, ConcurrentLinkedQueue<Response>> outputEventResponses = new HashMap<>();

    private ConcurrentLinkedQueue<Request> inputEventQueue = new ConcurrentLinkedQueue<>();
    private Map<Integer, ConcurrentLinkedQueue<Response>> inputEventResponses = new HashMap<>();

    private Map<Address, Boolean> isBusy = new ConcurrentHashMap<>();

    private Thread acceptThread;
    private Thread mainLogicThread;
    private Map<Integer, Thread> connectedToServerThreads = new HashMap<>();
    private Map<Integer, Thread> acceptedClientsThreads = new HashMap<>();
    private Map<Integer, Thread> pingAndHeartBeatThreads = new HashMap<>();
    /**
     * x  -- time in millis when last "pong" was received
     * 0  -- ping was not sent yet
     * -1 -- ping for this server is in event queue
     */
    private ConcurrentHashMap<Properties.ServerDescr, Long> lastPings = new ConcurrentHashMap<>();

    // Raft fields
    volatile private Role role;
    volatile private int currentLeaderId = 1; // for debug
    private MyStateMachine stateMachine = new MyStateMachine();
    private PersistentState persistentState;
    private VolatileState volatileState;
    private LeaderState leaderState;

    public Server(Integer id, Properties properties, Role role) throws IOException {
        this.id = id;
        this.properties = properties;
        Properties.ServerDescr serverDescr = properties.getServerDescrById(id);
        socket = new ServerSocket(serverDescr.address.port);
        socket.setReuseAddress(true);
        for (Properties.ServerDescr otherServerDescr : properties.serverDescrs) {
            addressMap.put(otherServerDescr.id, otherServerDescr.address);
        }
        initAcceptThread();

        this.role = role;
        String logFileName = "dkvs_" + id + ".log";
        persistentState = new PersistentState(logFileName);
        volatileState = new VolatileState();
        leaderState = new LeaderState(id, persistentState, properties);
    }

    private void initMainLogicThread() {
        mainLogicThread = new Thread(() -> {
            while (!Thread.interrupted()) {
                Request inputRequest = inputEventQueue.poll();
                if (inputRequest != null) {
                    processInputRequest(inputRequest);
                }
            }
        });
        mainLogicThread.start();
    }

    private void initAcceptedConnection(Socket clientSocket) {
        int clientSocketPort = clientSocket.getPort();
        inputEventResponses.put(clientSocketPort, new ConcurrentLinkedQueue<>());
        Thread acceptedClientThread = new Thread(() -> {
            try (Reader clientReader = new InputStreamReader(clientSocket.getInputStream());
                 Writer clientWriter = new OutputStreamWriter(clientSocket.getOutputStream())) {
                while (!Thread.interrupted() && !clientSocket.isClosed()) {
                    Request request = null;
                    int c = clientReader.read();
                    if (c == 'p') {
                        Log.d("Server." + id, "inputRequest: Ping from " + clientSocketPort);
                        request = Ping.read(clientReader);
                        Ping.Result result = new Ping.Result();
                        result.writeToWriter(clientWriter);
                        continue;
                    } else if (c == 'a') {
                        Log.d("Server." + id, "inputRequest: AppendEntries from " + clientSocketPort);
                        request = AppendEntries.read(clientReader);
                    } else if (c == 'g') {
                        Log.d("Server." + id, "inputRequest: Get from " + clientSocketPort);
                        request = Get.read(clientReader);
                        String name = ((Get) request).name;
                        Get.Result result = new Get.Result(name, stateMachine.get(name));
                        result.writeToWriter(clientWriter);
                        continue;
                    } else if (c == 'd' || c == 's') {
                        String type = c == 'd' ? "Delete" : "Set";
                        Log.d("Server." + id, "inputRequest: " + type + " from " + clientSocketPort);
                        if (c == 'd') {
                            request = Delete.read(clientReader);
                        } else {
                            request = Set.read(clientReader);
                        }
                        if (role != Role.LEADER) {
                            doRequest(addressMap.get(currentLeaderId), request, new Callback<Response>() {
                                @Override
                                public void onSuccess(Response result) {
                                    Log.d("Server." + id, type + ".onSuccess" + result.toString() + "(redirected)");
                                    try {
                                        result.writeToWriter(clientWriter);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }

                                @Override
                                public void onError() {
                                    Log.d("Server." + id, type + ".onError(redirected)");
                                    try {
                                        new Response.Error().writeToWriter(clientWriter);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                            continue;
                        }
                    } else if (c == 0 || c == 4) {
                        break;
                    }
                    if (request != null) {
                        request.setPort(clientSocket.getPort());
                        inputEventQueue.add(request);
                        Response response;
                        do {
                            response = inputEventResponses.get(clientSocketPort).poll();
                        } while (response == null);
                        response.writeToWriter(clientWriter);
                    }
                }
            } catch (IOException e) {
                Log.e("Server." + id, "inputRequest failed(" + e.getMessage() + ")");
            }
            Log.d("Server." + id, " disconnect from port " + clientSocketPort);
            inputEventResponses.remove(clientSocketPort);
            acceptedClientsThreads.remove(clientSocketPort);
        });
        acceptedClientsThreads.put(clientSocketPort, acceptedClientThread);
        acceptedClientThread.start();
    }

    private void initAcceptThread() {
        acceptThread = new Thread(() -> {
            try {
                while (!Thread.interrupted()) {
                    Socket clientSocket = socket.accept();
                    Log.d("Server." + id,
                            "accepted connection from " + clientSocket.getInetAddress().getHostAddress()
                            + " on port " + clientSocket.getPort()
                    );
                    initAcceptedConnection(clientSocket);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        acceptThread.start();
    }

    private void initConnectThreads() {
        for (Properties.ServerDescr serverDescr : properties.serverDescrs) {
            if (id == serverDescr.id) {
                continue;
            }
            Thread connectThread = new Thread(() -> {
                while (!Thread.interrupted()) {
                    try (Socket serverSocket = new Socket(serverDescr.address.ip, serverDescr.address.port)) {
                        serverSocket.setSoTimeout((int) properties.timeout * properties.serverDescrs.size());
                        Log.d("Server." + id, "connected to Server." + serverDescr.id);
                        try (Reader serverReader = new InputStreamReader(serverSocket.getInputStream());
                             Writer serverWriter = new PrintWriter(serverSocket.getOutputStream())) {
                            while (!serverSocket.isClosed() && !Thread.interrupted()) {
                                Request outputRequest = outputEventQueues.get(serverDescr.address).poll();
                                if (outputRequest == null) {
                                    continue;
                                }
                                try {
                                    processOutputRequest(outputRequest, serverDescr.address, serverReader, serverWriter);
                                } catch (SocketTimeoutException exception) {
                                    outputEventResponses.get(serverDescr.address).add(new Response.Error());
                                    Log.e("Server." + id, "timeout on request to Server." + serverDescr.id);
//                                    exception.printStackTrace();
                                    break;
                                }
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            connectedToServerThreads.put(serverDescr.id, connectThread);
            outputEventQueues.put(serverDescr.address, new ConcurrentLinkedQueue<>());
            outputEventResponses.put(serverDescr.address, new ConcurrentLinkedQueue<>());
            connectThread.start();
        }
    }

    private void initPingThreads() {
        for (Properties.ServerDescr serverDescr : properties.serverDescrs) {
            if (id == serverDescr.id) {
                continue;
            }
            Thread pingThread = new Thread(() -> {
                while (!Thread.interrupted()) {
                    if (role == Role.LEADER) {
                        sendHeartBeat(serverDescr);
                        continue;
                    }

                    Long lastTimeout = lastPings.getOrDefault(serverDescr, 0L);
                    if (isBusy.getOrDefault(serverDescr.address, false) || System.currentTimeMillis() - lastTimeout < properties.timeout) {
                        continue;
                    }
                    doRequest(serverDescr.address, new Ping(), new Callback<Ping.Result>() {
                        @Override
                        public void onPreExecute() {
                            super.onPreExecute();
                        }

                        @Override
                        public void onSuccess(Ping.Result result) {
//                            Log.d("Server." + id, "Ping.onSuccess(from server." + serverDescr.id + ")");
                            lastPings.put(serverDescr, System.currentTimeMillis());
                        }

                        @Override
                        public void onError() {
//                            Log.e("Server." + id, "Ping.onError");
                            lastPings.put(serverDescr, System.currentTimeMillis());
                        }
                    });
                }
            });
            pingAndHeartBeatThreads.put(serverDescr.id, pingThread);
            pingThread.start();
        }
    }

    private void processInputRequest(@NotNull Request request) {
        Log.d("Server." + id, "processInputRequest " + request.toString());
        int port = request.getPort();
        if (request instanceof AppendEntries) {
            AppendEntries appendEntries = (AppendEntries) request;
            currentLeaderId = appendEntries.leaderId;
            AppendEntries.Result result;
            int currentTerm = persistentState.currentTerm;
            List<LogEntry> log = persistentState.log;
            int prevLogIndex = appendEntries.prevLogIndex;
            int prevLogTerm = appendEntries.prevLogTerm;
            if (currentTerm > appendEntries.term) {
                result = new AppendEntries.Result(false, currentTerm);
            } else if (log.size() <= prevLogIndex || (prevLogIndex != -1 && log.get(prevLogIndex).term != prevLogTerm)) {
                result = new AppendEntries.Result(false, currentTerm);
            } else {
                int i;
                for (i = 0; i < appendEntries.entries.size(); i++) {
                    LogEntry newEntry = appendEntries.entries.get(i);
                    int newEntryIndex = prevLogIndex + i + 1;
                    if (newEntryIndex >= log.size()) {
                        break;
                    }
                    LogEntry currentEntry = log.get(newEntryIndex);
                    if (newEntry.term != currentEntry.term) {
                        log.subList(newEntryIndex, log.size()).clear();
                        break;
                    }
                }
                if (i != appendEntries.entries.size()) {
                    log.addAll(appendEntries.entries.subList(i, appendEntries.entries.size()));
                }
                if (appendEntries.leaderCommit > volatileState.commitIndex) {
                    volatileState.commitIndex = Math.min(appendEntries.leaderCommit, log.size());
                    applyLogCommands();
                }
                result = new AppendEntries.Result(true, appendEntries.term);
            }
            if (currentTerm < appendEntries.term) {
                persistentState.currentTerm = appendEntries.term;
            }
            persistentState.saveToStorage();
            currentLeaderId = appendEntries.leaderId;
            inputEventResponses.get(port).add(result);
        } else if (request instanceof RequestVote) {
            RequestVote requestVote = (RequestVote) request;

        } else if (request instanceof Ping) {
            inputEventResponses.get(port).add(new Ping.Result());
        } else if (request instanceof Delete || request instanceof Set) {
            LogEntry logEntry;
            if (request instanceof Delete) {
                logEntry = new LogEntry(persistentState.currentTerm, new DeleteCommand(((Delete) request).name));
            } else {
                logEntry = new LogEntry(persistentState.currentTerm, new SetCommand(((Set) request).name, ((Set) request).value));
            }
            persistentState.log.add(logEntry);
            persistentState.saveToStorage();
            int serversCount = properties.serverDescrs.size();
            CountDownLatch countDownLatch = new CountDownLatch(serversCount / 2 + 1);
            for (Properties.ServerDescr serverDescr : properties.serverDescrs) {
                updateLogOnFollower(serverDescr.id, countDownLatch::countDown);
            }
            try {
                countDownLatch.await();
                if (request instanceof Delete) {
                    inputEventResponses.get(port).add(new Delete.Result(logEntry.command.getResult()));
                } else {
                    inputEventResponses.get(port).add(new Set.Result());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void processOutputRequest(
            Request outputRequest,
            Address address,
            Reader serverReader,
            Writer serverWriter
    ) throws IOException {
        if (outputRequest instanceof Ping) {
            Log.d("Server." + id, "outputRequest: Ping to " + address);
            outputRequest.writeToWriter(serverWriter);
            outputEventResponses.get(address).add(Ping.Result.read(serverReader));
        } else if (outputRequest instanceof AppendEntries) {
            Log.d("Server." + id, "outputRequest: AppendEntries to " + address);
            outputRequest.writeToWriter(serverWriter);
            outputEventResponses.get(address).add(AppendEntries.Result.read(serverReader));
        } else if (outputRequest instanceof Delete) {
            Log.d("Server." + id, "outputRequest: Delete to " + address);
            outputRequest.writeToWriter(serverWriter);
            outputEventResponses.get(address).add(Delete.Result.read(serverReader));
        } else if (outputRequest instanceof Set) {
            Log.d("Server." + id, "outputRequest: Set to " + address);
            outputRequest.writeToWriter(serverWriter);
            outputEventResponses.get(address).add(Set.Result.read(serverReader));
        }
    }

    private boolean checkCommitIndex(int commitIndex) {
        int cnt = 0;
        for (Integer matchIndex : leaderState.matchIndex.values()) {
            if (matchIndex >= commitIndex) {
                cnt++;
            }
        }
        return cnt >= properties.serverDescrs.size() / 2 + 1;
    }

    private void applyLogCommands() {
        for (int i = volatileState.lastApplied; i < volatileState.commitIndex; i++) {
            persistentState.log.get(i).command.apply(stateMachine);
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
    }

    private void updateLogOnFollower(int followerId, Runnable onSuccess) {
        updateLogOnFollower(followerId, onSuccess, false);
    }

    private void updateLogOnFollower(int followerId, Runnable onSuccess, boolean isHeartBeat) {
        if (followerId == id) {
            leaderState.matchIndex.put(id, persistentState.log.size());
            updateCommitIndexAndApplyToStateMachine();
            if (onSuccess != null) {
                onSuccess.run();
            }
            return;
        }
        int nextIndex = leaderState.nextIndex.get(followerId);
        List<LogEntry> log = persistentState.log;
        AppendEntries appendEntries = new AppendEntries(
                persistentState.currentTerm,
                id,
                nextIndex - 1,
                nextIndex != 0 ? log.get(nextIndex - 1).term : 0,
                new ArrayList<>(log.subList(nextIndex, log.size())),
                volatileState.commitIndex
        );
        final int oldSize = log.size();
        if (role == Role.LEADER) {
            Address address = addressMap.get(followerId);
            doRequest(address, appendEntries, new Callback<AppendEntries.Result>() {
                @Override
                public void onSuccess(AppendEntries.Result result) {
                    if (result.success) {
                        leaderState.matchIndex.put(followerId, oldSize);
                        leaderState.nextIndex.put(followerId, oldSize);
                        updateCommitIndexAndApplyToStateMachine();
                        if (onSuccess != null) {
                            onSuccess.run();
                        }
                    } else {
                        int newNextIndex = appendEntries.prevLogIndex;
                        leaderState.nextIndex.put(followerId, newNextIndex);
                        updateLogOnFollower(followerId, onSuccess);
                    }
                }

                @Override
                public void onError() {
//                    if (!isHeartBeat) {
                        updateLogOnFollower(followerId, onSuccess);
//                    }
                }
            });
        }
    }

    private void sendHeartBeat(Properties.ServerDescr serverDescr) {
        updateLogOnFollower(serverDescr.id, null, true);
    }

    private synchronized <T extends Request> void doRequest(Address address, T request, Callback callback) {
//        Socket
        isBusy.put(address, true);
        callback.onPreExecute();
        outputEventQueues.get(address).add(request);
        ConcurrentLinkedQueue<Response> responseQueue = outputEventResponses.get(address);
        Response response;
        do {
            response = responseQueue.poll();
        } while (response == null);
        isBusy.put(address, false);
        if (response instanceof Response.Error) {
            callback.onError();
        } else {
            callback.onSuccess(response);
        }
    }

    private void changeRole(Role newRole) {

    }

    public void start() {
        initConnectThreads();
        initMainLogicThread();
        initPingThreads();
    }

    @Override
    public void close() throws Exception {
        acceptThread.interrupt();
        for (Thread thread : acceptedClientsThreads.values()) {
            thread.interrupt();
        }
        for (Thread thread : connectedToServerThreads.values()) {
            thread.interrupt();
        }
        for (Thread thread : pingAndHeartBeatThreads.values()) {
            thread.interrupt();
        }
        if (socket != null) {
            socket.close();
        }
    }
}
