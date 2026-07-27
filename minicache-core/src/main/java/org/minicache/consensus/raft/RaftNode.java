package org.minicache.consensus.raft;

import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RaftNode {
    private final String nodeId;
    private final List<String> clusterNodes;
    private NodeState state;
    private int currentTerm;
    private String votedFor;
    private final ScheduledExecutorService timerExecutor;
    private ScheduledFuture<?> electionTask;
    private final RaftListener stateMachine;
    private final ScheduledExecutorService heartbeatExecutor;
    private ScheduledFuture<?> heartbeatTask;
    private final List<LogEntry> logList;
    private int commitIndex;
    private int lastApplied;
    private final Map<String, Integer> nextIndex;
    private final Map<String, Integer> matchIndex;
    private final Map<Integer, CompletableFuture<Boolean>> pendingProposals;
    private final Logger logger;
    private ServerSocket serverSocket;
    private String currentLeader;
    private final AtomicBoolean isRunning;
    private final boolean isBinaryMsg;
    private final ReentrantLock lock;
    private final int logBatchSize;
    private final Map<String, Socket> connectionPool;
    private final Map<String, DataOutputStream> outputStreamPool;
    private final Map<String, DataInputStream> inputStreamPool;

    public RaftNode(String nodeId, List<String> clusterNodes, RaftListener stateMachine, Logger logger,
                    Boolean isBinaryMsg, Integer logBatchSize) {
        this.nodeId = nodeId;
        this.clusterNodes = clusterNodes;
        this.stateMachine = stateMachine;
        this.timerExecutor = Executors.newSingleThreadScheduledExecutor();
        this.state = NodeState.FOLLOWER;
        this.currentTerm = 0;
        this.votedFor = null;
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        this.logList = new CopyOnWriteArrayList<>();
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        this.pendingProposals = new ConcurrentHashMap<>();
        this.isRunning = new AtomicBoolean(true);
        this.logger = logger;
        this.lock = new ReentrantLock();
        this.logBatchSize = logBatchSize;
        this.connectionPool = new ConcurrentHashMap<>();
        this.outputStreamPool = new ConcurrentHashMap<>();
        this.inputStreamPool = new ConcurrentHashMap<>();

        this.isBinaryMsg = isBinaryMsg;
        if (this.isBinaryMsg) {
            this.logger.info("[RAFT] Using Binary-based Protocol");
        } else {
            this.logger.info("[RAFT] Using Text-based Protocol");
        }

        this.logList.add(new LogEntry(0, 0, "NO_OP"));
    }

    public void start() {
        lock.lock();
        try {
            for (String node : clusterNodes) {
                if (node.contains(this.nodeId)) {
                    int port = Integer.parseInt(node.split(":")[1]);
                    this.listen(port);
                    break;
                }
            }
            resetElectionTimeout();
        } finally {
            lock.unlock();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    public NodeState getState() {
        lock.lock();
        try {
            return this.state;
        } finally {
            lock.unlock();
        }
    }

    public String getLeader() {
        lock.lock();
        try {
            return this.currentLeader;
        } finally {
            lock.unlock();
        }
    }

    public String getNodeId() {
        return this.nodeId;
    }

    public int getCurrentTerm() {
        lock.lock();
        try {
            return this.currentTerm;
        } finally {
            lock.unlock();
        }
    }

    public void resetElectionTimeout() {
        lock.lock();
        try {
            if (electionTask != null && !electionTask.isCancelled()) {
                electionTask.cancel(true);
            }
            var electionTimeout = 1000 + ThreadLocalRandom.current().nextInt(1000);
            electionTask = timerExecutor.schedule(() -> {
                Thread.startVirtualThread(this::startElection);
            }, electionTimeout, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    private void startElection() {
        int termToVote;
        int quorum = (clusterNodes.size() / 2) + 1;
        AtomicInteger grantedVotes = new AtomicInteger(1);

        lock.lock();
        try {
            if (this.state == NodeState.LEADER) return;

            this.state = NodeState.CANDIDATE;
            this.currentTerm++;
            this.votedFor = this.nodeId;
            termToVote = this.currentTerm;

            stateMachine.onBecomeCandidate();
        } finally {
            lock.unlock();
        }

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (String targetNode : clusterNodes) {
                if (shouldSkipNode(targetNode)) continue;

                executor.submit(() -> {
                    boolean voteGranted = this.isBinaryMsg
                            ? sendRequestVoteRpcBinary(targetNode, termToVote, this.nodeId)
                            : sendRequestVoteRpc(targetNode, termToVote, this.nodeId);
                    if (voteGranted) {
                        int currentVotes = grantedVotes.incrementAndGet();
                        checkElectionResult(currentVotes, quorum);
                    }
                });
            }
        }

        lock.lock();
        try {
            if (this.state == NodeState.CANDIDATE) {
                resetElectionTimeout();
            }
        } finally {
            lock.unlock();
        }
    }

    private synchronized Socket getOrCreateConnection(String targetNode) throws IOException {
        Socket socket = connectionPool.get(targetNode);
        if (socket == null || socket.isClosed() || !socket.isConnected() || socket.isOutputShutdown()) {
            String[] parts = targetNode.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            socket.connect(new InetSocketAddress(host, port), 1000);
            socket.setSoTimeout(3000);

            connectionPool.put(targetNode, socket);
            outputStreamPool.put(targetNode, new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 2048)));
            inputStreamPool.put(targetNode, new DataInputStream(new BufferedInputStream(socket.getInputStream(), 2048)));
        }
        return socket;
    }

    private synchronized void closeAndRemoveConnection(String targetNode) {
        try {
            Socket socket = connectionPool.remove(targetNode);
            outputStreamPool.remove(targetNode);
            inputStreamPool.remove(targetNode);
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException ignored) {
        }
    }

    private void checkElectionResult(int currentVotes, int quorum) {
        lock.lock();
        try {
            if (this.state == NodeState.CANDIDATE && currentVotes >= quorum) {
                this.state = NodeState.LEADER;
                if (electionTask != null) {
                    electionTask.cancel(true);
                }
                int lastLogIndex = logList.size() - 1;
                for (String targetNode : clusterNodes) {
                    nextIndex.put(targetNode, lastLogIndex + 1);
                    matchIndex.put(targetNode, 0);
                }

                if (this.stateMachine != null) {
                    stateMachine.onBecomeLeader();
                }
                startHeartbeatLoop();
            }
        } finally {
            lock.unlock();
        }
    }

    private void startHeartbeatLoop() {
        this.logger.info("Start Heartbeat Loop...");
        if (heartbeatTask != null && !heartbeatTask.isCancelled()) {
            heartbeatTask.cancel(true);
        }
        heartbeatTask = heartbeatExecutor.scheduleAtFixedRate(() -> {
            Thread.startVirtualThread(this::broadcastAppendEntries);
        }, 0, 150, TimeUnit.MILLISECONDS);
    }

    private void broadcastAppendEntries() {
        int termToSend;
        NodeState currentState;
        lock.lock();
        try {
            termToSend = this.currentTerm;
            currentState = this.state;
        } finally {
            lock.unlock();
        }

        if (currentState != NodeState.LEADER) {
            if (heartbeatTask != null) heartbeatTask.cancel(true);
            return;
        }

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (String targetNode : clusterNodes) {
                if (shouldSkipNode(targetNode)) continue;

                executor.submit(() -> {
                    if (this.isBinaryMsg) {
                        syncLogWithFollowerBinary(targetNode, termToSend);
                    } else {
                        syncLogWithFollower(targetNode, termToSend);
                    }
                });
            }
        }
    }

    private void syncLogWithFollower(String targetNode, int termToSend) {
        int prevLogIndex;
        int prevLogTerm;
        int leaderCommit;
        String entriesData = "";

        int nextIdx = nextIndex.getOrDefault(targetNode, 1);
        prevLogIndex = nextIdx - 1;
        prevLogTerm = logList.get(prevLogIndex).term();
        leaderCommit = this.commitIndex;

        if (logList.size() > nextIdx) {
            List<LogEntry> subList = logList.subList(nextIdx, logList.size());
            StringBuilder sb = new StringBuilder();
            for (LogEntry entry : subList) {
                sb.append(entry.index())
                        .append("#")
                        .append(entry.term())
                        .append("#")
                        .append(entry.command())
                        .append(";");
            }
            entriesData = sb.toString();
        }

        String[] parts = targetNode.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(host, port), 1000);
            try (PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                writer.println(String.format("APPEND_ENTRIES|%d|%s|%d|%d|%d|%s",
                        termToSend, nodeId, prevLogIndex, prevLogTerm, leaderCommit, entriesData));

                String response = reader.readLine();
                if (response != null) {
                    String[] tokens = response.split("\\|", 4);
                    if ("APPEND_REPLY".equals(tokens[0])) {
                        int responderTerm = Integer.parseInt(tokens[1]);
                        boolean success = Boolean.parseBoolean(tokens[2]);
                        int followerMatchIndex = Integer.parseInt(tokens[3]);

                        if (responderTerm > termToSend) {
                            stepDownToFollower(responderTerm);
                            return;
                        }

                        if (success) {
                            matchIndex.put(targetNode, followerMatchIndex);
                            nextIndex.put(targetNode, followerMatchIndex + 1);
                            checkAndAdvanceCommitIndex();
                        } else {
                            nextIndex.put(targetNode, Math.max(1, nextIdx - 1));
                        }
                    }
                }
            }
        } catch (Exception ignored) {
        }
    }

    private void syncLogWithFollowerBinary(String targetNode, int termToSend) {
        int prevLogIndex;
        int prevLogTerm;
        int leaderCommit;
        String entriesData = "";

        int nextIdx = nextIndex.getOrDefault(targetNode, 1);
        if (nextIdx > logList.size()) {
            nextIdx = logList.size();
        }

        prevLogIndex = nextIdx - 1;
        prevLogTerm = logList.get(prevLogIndex).term();
        leaderCommit = this.commitIndex;

        int unSyncedCount = logList.size() - nextIdx;
        if (unSyncedCount >= logBatchSize || (unSyncedCount > 0 && nextIdx <= commitIndex)) {
            List<LogEntry> subList = logList.subList(nextIdx, logList.size());
            StringBuilder sb = new StringBuilder();
            for (LogEntry entry : subList) {
                sb.append(entry.index())
                        .append("#")
                        .append(entry.term())
                        .append("#")
                        .append(entry.command())
                        .append(";");
            }
            entriesData = sb.toString();
        }

        try {
            Socket socket = getOrCreateConnection(targetNode);
            synchronized (socket) {
                DataOutputStream out = outputStreamPool.get(targetNode);
                DataInputStream in = inputStreamPool.get(targetNode);

                out.writeByte(0x4C);
                out.writeInt(termToSend);
                out.writeUTF(nodeId);
                out.writeInt(prevLogIndex);
                out.writeInt(prevLogTerm);
                out.writeInt(leaderCommit);

                byte[] entriesDataBytes = (!entriesData.isEmpty())
                        ? entriesData.getBytes(StandardCharsets.UTF_8)
                        : new byte[0];
                byte[] compressedBytes = new byte[0];
                if (entriesDataBytes.length > 0) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
                        gzipOut.write(entriesDataBytes);
                    }
                    compressedBytes = baos.toByteArray();
                }
                out.writeInt(compressedBytes.length);
                if (compressedBytes.length > 0) {
                    out.write(compressedBytes);
                }
                out.flush();

                byte magic = in.readByte();
                if (magic == 0x4C) {
                    int responderTerm = in.readInt();
                    boolean success = in.readBoolean();
                    int followerMatchIndex = in.readInt();

                    if (responderTerm > termToSend) {
                        out.flush();
                        stepDownToFollower(responderTerm);
                        return;
                    }

                    if (success) {
                        if (!entriesData.isEmpty()) {
                            int lastSentIndex = logList.size() - 1;
                            matchIndex.put(targetNode, lastSentIndex);
                            nextIndex.put(targetNode, lastSentIndex + 1);
                            checkAndAdvanceCommitIndex();
                        } else {
                            matchIndex.put(targetNode, Math.max(matchIndex.getOrDefault(targetNode, 0), followerMatchIndex));
                            nextIndex.put(targetNode, Math.max(nextIndex.getOrDefault(targetNode, 1), followerMatchIndex + 1));
                        }
                    } else {
                        if (followerMatchIndex < nextIdx) {
                            nextIndex.put(targetNode, Math.max(1, followerMatchIndex + 1));
                        } else {
                            nextIndex.put(targetNode, Math.max(1, nextIdx - 1));
                        }
                    }
                }
                out.flush();
            }
        } catch (Exception ex) {
            closeAndRemoveConnection(targetNode);
        }
    }

    public void propose(String command) {
        lock.lock();
        try {
            if (this.state != NodeState.LEADER) return;
            int entryIndex = logList.size();
            logList.add(new LogEntry(entryIndex, this.currentTerm, command));
        } finally {
            lock.unlock();
        }
    }

    private void checkAndAdvanceCommitIndex() {
        lock.lock();
        try {
            int quorum = (clusterNodes.size() / 2) + 1;
            int lastLogIndex = logList.size() - 1;

            for (int index = commitIndex + 1; index <= lastLogIndex; index++) {
                if (logList.get(index).term() == currentTerm) {
                    int count = 1;
                    for (String targetNode : clusterNodes) {
                        if (shouldSkipNode(targetNode)) continue;
                        if (matchIndex.getOrDefault(targetNode, 0) >= index) {
                            count++;
                        }
                    }
                    if (count >= quorum) {
                        this.commitIndex = index;
                        applyLogsToStateMachine();

                        CompletableFuture<Boolean> future = pendingProposals.remove(index);
                        if (future != null && !future.isDone()) {
                            future.complete(true);
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void applyLogsToStateMachine() {
        while (commitIndex > lastApplied) {
            lastApplied++;
            LogEntry entry = logList.get(lastApplied);
            if (this.stateMachine != null && !"NO_OP".equals(entry.command())) {
                stateMachine.onLogCommitted(entry.command());
            }
        }
    }

    public boolean handleAppendEntries(int leaderTerm, String leaderId, int prevLogIndex, int prevLogTerm, int leaderCommit, String entriesData) {
        lock.lock();
        try {
            if (leaderTerm < this.currentTerm)
                return false;

            if (leaderTerm > this.currentTerm || this.state == NodeState.CANDIDATE) {
                this.currentTerm = leaderTerm;
                this.state = NodeState.FOLLOWER;
                this.votedFor = null;
                stateMachine.onBecomeFollower();
            }

            this.currentLeader = leaderId;
            resetElectionTimeout();

            if (prevLogIndex >= logList.size() || logList.get(prevLogIndex).term() != prevLogTerm) {
                return false;
            }

            if (entriesData != null && !entriesData.isBlank()) {
                String[] rawEntries = entriesData.split(";");
                int writeIndex = prevLogIndex + 1;

                for (String rawEntry : rawEntries) {
                    if (rawEntry.isBlank()) continue;

                    String[] tokens = rawEntry.split("#", 3);
                    int index = Integer.parseInt(tokens[0]);
                    int term = Integer.parseInt(tokens[1]);
                    String cmd = tokens[2];

                    LogEntry newEntry = new LogEntry(index, term, cmd);
                    if (writeIndex < logList.size()) {
                        if (logList.get(writeIndex).term() != term) {
                            logList.subList(writeIndex, logList.size()).clear();
                            logList.add(newEntry);
                        }
                    } else {
                        logList.add(newEntry);
                    }
                    writeIndex++;
                }
            }

            if (leaderCommit > this.commitIndex) {
                this.commitIndex = Math.min(leaderCommit, logList.size() - 1);
                applyLogsToStateMachine();
            }

            return true;
        } finally {
            lock.unlock();
        }
    }

    private boolean sendRequestVoteRpc(String targetNode, int term, String candidateId) {
        String[] parts = targetNode.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(host, port), 1000);
            try (PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                writer.println("REQUEST_VOTE|" + term + "|" + candidateId);
                String response = reader.readLine();
                if (response != null) {
                    String[] tokens = response.split("\\|");
                    if ("VOTE_RESPONSE".equals(tokens[0])) {
                        int responderTerm = Integer.parseInt(tokens[1]);
                        if (responderTerm > term) {
                            stepDownToFollower(responderTerm);
                            return false;
                        }
                        return Boolean.parseBoolean(tokens[2]);
                    }
                }
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    private boolean sendRequestVoteRpcBinary(String targetNode, int term, String candidateId) {
        String[] parts = targetNode.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(host, port), 1000);
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 512));
                 DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 512))) {

                out.writeByte(0x56);
                out.writeInt(term);
                out.writeUTF(candidateId);
                out.flush();

                byte magic = in.readByte();
                if (magic == 0x56) {
                    int responderTerm = in.readInt();
                    boolean voteGranted = in.readBoolean();
                    if (responderTerm > term) {
                        out.flush();
                        stepDownToFollower(responderTerm);
                        return false;
                    }
                    out.flush();
                    return voteGranted;
                }
                out.flush();
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    public void stepDownToFollower(int newerTerm) {
        lock.lock();
        try {
            if (newerTerm > this.currentTerm) {
                this.currentTerm = newerTerm;
                this.state = NodeState.FOLLOWER;
                this.votedFor = null;
                if (heartbeatTask != null) {
                    heartbeatTask.cancel(true);
                }
                pendingProposals.forEach((index, future) -> {
                    if (!future.isDone()) {
                        future.complete(false);
                    }
                });
                pendingProposals.clear();
                resetElectionTimeout();
                stateMachine.onBecomeFollower();
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean handleRequestVote(int candidateTerm, String candidateId, int candidateLastLogIndex,
                                     int candidateLastLogTerm) {
        lock.lock();
        try {
            // Từ chối ngay nếu Candidate có Term nhỏ hơn Term hiện tại
            if (candidateTerm < this.currentTerm) {
                return false;
            }

            // Nếu Candidate có Term lớn hơn, chuyển ngay về FOLLOWER và reset phiếu bầu
            if (candidateTerm > this.currentTerm) {
                this.currentTerm = candidateTerm;
                this.state = NodeState.FOLLOWER;
                this.votedFor = null;
                stateMachine.onBecomeFollower();
            }

            // Lấy thông tin Log cuối cùng của Node hiện tại trên RAM
            int myLastLogIndex = this.logList.size() - 1;
            int myLastLogTerm = (myLastLogIndex >= 0) ? this.logList.get(myLastLogIndex).term() : 0;

            // Kiểm tra raft election safety: Log của Candidate phải đầy đủ/mới hơn hoặc bằng log của node này
            boolean isLogUpToDate = false;
            if (candidateLastLogTerm > myLastLogTerm) {
                isLogUpToDate = true;
            } else if (candidateLastLogTerm == myLastLogTerm && candidateLastLogIndex >= myLastLogIndex) {
                isLogUpToDate = true;
            }

            // Chỉ cho vote nếu chưa cấp phiếu cho ai khác trong Term này và Log của Candidate đạt chuẩn
            if ((this.votedFor == null || this.votedFor.equals(candidateId)) && isLogUpToDate) {
                this.votedFor = candidateId;
                resetElectionTimeout();
                return true;
            }

            return false;
        } finally {
            lock.unlock();
        }
    }

    private boolean shouldSkipNode(String targetNode) {
        return targetNode.startsWith(nodeId + ":")
                || targetNode.contains("127.0.0.1")
                || targetNode.contains("localhost");
    }

    public void listen(int port) {
        Thread.startVirtualThread(() -> {
            try {
                serverSocket = new ServerSocket();
                serverSocket.setReuseAddress(true);
                serverSocket.bind(new InetSocketAddress(port));
                this.logger.info("[RAFT] Internal Server is listening on port: {}", port);
                while (isRunning.get()) {
                    Socket socket = serverSocket.accept();
                    Thread.startVirtualThread(() -> {
                        if (this.isBinaryMsg) {
                            handleRaftConnectionBinary(socket);
                        } else {
                            handleRaftConnection(socket);
                        }
                    });
                }
            } catch (IOException e) {
                this.logger.error("[RAFT] Failed to start Raft Server on port " + port, e);
            }
        });
    }

    private void handleRaftConnection(Socket socket) {
        try (socket;
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            String line = reader.readLine();
            if (line == null) return;

            String[] tokens = line.split("\\|", 7);
            String messageType = tokens[0];

            if ("REQUEST_VOTE".equals(messageType)) {
                int term = Integer.parseInt(tokens[1]);
                String candidateId = tokens[2];

                int myLastLogIndex = this.logList.size() - 1;
                int myLastLogTerm = (myLastLogIndex >= 0) ? this.logList.get(myLastLogIndex).term() : 0;
                boolean voteGranted = this.handleRequestVote(term, candidateId, myLastLogIndex, myLastLogTerm);

                writer.println("VOTE_RESPONSE|" + this.currentTerm + "|" + voteGranted);
                this.logger.info("[RAFT] Processed RequestVote from node [{}]: granted={}", candidateId, voteGranted);

            } else if ("APPEND_ENTRIES".equals(messageType)) {
                int leaderTerm = Integer.parseInt(tokens[1]);
                String leaderId = tokens[2];
                int prevLogIndex = Integer.parseInt(tokens[3]);
                int prevLogTerm = Integer.parseInt(tokens[4]);
                int leaderCommit = Integer.parseInt(tokens[5]);
                String entriesData = tokens.length > 6 ? tokens[6] : "";

                boolean success = this.handleAppendEntries(leaderTerm, leaderId, prevLogIndex, prevLogTerm, leaderCommit, entriesData);
                writer.println("APPEND_REPLY|" + this.currentTerm + "|" + success + "|" + (logList.size() - 1));
            }
        } catch (Exception e) {
            this.logger.error("[RAFT] Error processing Raft RPC", e);
        }
    }

    private void handleRaftConnectionBinary(Socket socket) {
        try (socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 2048));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 2048))) {
            while (isRunning.get() && !socket.isClosed()) {
                int magicByte;
                try {
                    magicByte = in.readByte();
                } catch (EOFException eof) {
                    break;
                }

                if (magicByte == 0x56) {
                    int term = in.readInt();
                    String candidateId = in.readUTF();

                    int myLastLogIndex = this.logList.size() - 1;
                    int myLastLogTerm = (myLastLogIndex >= 0) ? this.logList.get(myLastLogIndex).term() : 0;
                    boolean voteGranted = this.handleRequestVote(term, candidateId, myLastLogIndex, myLastLogTerm);

                    out.writeByte(0x56);
                    out.writeInt(this.currentTerm);
                    out.writeBoolean(voteGranted);
                    out.flush();

                    this.logger.info("[RAFT] Processed RequestVote from node [{}]: granted={}", candidateId,
                            voteGranted);
                } else if (magicByte == 0x4C) {
                    int leaderTerm = in.readInt();
                    String leaderId = in.readUTF();
                    int prevLogIndex = in.readInt();
                    int prevLogTerm = in.readInt();
                    int leaderCommit = in.readInt();
                    int entriesDataLength = in.readInt();

                    if (entriesDataLength < 0 || entriesDataLength > 10 * 1024 * 1024) {
                        break;
                    }

                    byte[] compressedBytes = new byte[entriesDataLength];
                    if (entriesDataLength > 0) {
                        in.readFully(compressedBytes);
                    }

                    String entriesData = "";
                    if (compressedBytes.length > 0) {
                        ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);
                        try (GZIPInputStream gzipIn = new GZIPInputStream(bais);
                             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

                            byte[] buffer = new byte[1024];
                            int len;
                            while ((len = gzipIn.read(buffer)) != -1) {
                                baos.write(buffer, 0, len);
                            }
                            entriesData = baos.toString(StandardCharsets.UTF_8);
                        }
                    }

                    boolean success = this.handleAppendEntries(leaderTerm, leaderId, prevLogIndex, prevLogTerm,
                            leaderCommit, entriesData);

                    out.writeByte(0x4C);
                    out.writeInt(this.currentTerm);
                    out.writeBoolean(success);
                    out.writeInt((logList.size() - 1));
                    out.flush();
                }
            }
        } catch (Exception e) {
            if (isRunning.get()) {
                this.logger.debug("[RAFT] Connection closed or error in Raft RPC handler: {}", e.getMessage());
            }
        }
    }

    public void stop() {
        this.logger.info("[RAFT] Graceful shutdown RaftNode...");

        isRunning.set(false);

        try {
            if (heartbeatTask != null) {
                heartbeatTask.cancel(true);
            }
            if (electionTask != null) {
                electionTask.cancel(true);
            }

            heartbeatExecutor.shutdownNow();
            timerExecutor.shutdownNow();
        } catch (Exception ignored) {
        }

        if (this.serverSocket != null && !this.serverSocket.isClosed()) {
            try {
                this.serverSocket.close();
            } catch (IOException ignored) {
            }
        }

        if (pendingProposals != null) {
            pendingProposals.forEach((index, future) -> {
                if (!future.isDone()) {
                    future.complete(false);
                }
            });
            pendingProposals.clear();
        }

        for (String targetNode : connectionPool.keySet()) {
            closeAndRemoveConnection(targetNode);
        }
    }
}