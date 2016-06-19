package nio;

import nio.model.LogEntry;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 18.06.16.
 */
class PersistentState {
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
            String s;
            while ((s = logReader.readLine()) != null) {
                if (!s.isEmpty()) {
                    log.add(Helper.gson.fromJson(s, LogEntry.class));
                }
            }
        }
        try (BufferedReader log2Reader = new BufferedReader(new FileReader(log2File))) {
            String termS = log2Reader.readLine();
            String votedS = log2Reader.readLine();
            if (termS != null && !termS.isEmpty()) {
                currentTerm = Integer.parseInt(termS);
                if (votedS != null) {
                    votedFor = Integer.parseInt(votedS);
                }
            }
        }
    }

    public void updateCurrentTerm(int newTerm) {
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            votedFor = null;
            saveTermAndVotedFor();
        }
    }

    public void voteAndIncrementTerm(int votedFor) {
        currentTerm++;
        this.votedFor = votedFor;
        saveTermAndVotedFor();
    }

    public void setVotedFor(int votedFor) {
        if (this.votedFor == null || this.votedFor != votedFor) {
            this.votedFor = votedFor;
            saveTermAndVotedFor();
        }
    }

    private void saveLog() {
        try {
            try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFileName))) {
                for (LogEntry logEntry : log) {
                    logWriter.write(Helper.gson.toJson(logEntry, LogEntry.class) + "\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveTermAndVotedFor() {
        try {
            try (BufferedWriter log2Writer = new BufferedWriter(new FileWriter(log2FileName))) {
                log2Writer.write(String.valueOf(currentTerm) + "\n");
                if (votedFor != null) {
                    log2Writer.write(String.valueOf(votedFor) + "\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public LogEntry getLogEntry(int i) {
        return log.get(i);
    }

    public int getLogSize() {
        return log.size();
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public List<LogEntry> getLogSuffix(int from) {
        return new ArrayList<>(log.subList(from, log.size()));
    }

    public void clearLogSuffix(int from) {
        log.subList(from, log.size()).clear();
        saveLog();
    }

    public Integer getVotedFor() {
        return votedFor;
    }

    public void addLogEntries(List<LogEntry> newLogEntries) {
        log.addAll(newLogEntries);
        saveLog();
    }

    public void addLogEntry(LogEntry newLogEntry) {
        log.add(newLogEntry);
        saveLog();
    }
}
