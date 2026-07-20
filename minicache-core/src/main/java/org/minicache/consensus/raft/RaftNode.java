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

    public RaftNode(String nodeId, List<String> clusterNodes, RaftListener stateMachine, Logger logger,
                    Boolean isBinaryMsg) {
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

        this.isBinaryMsg = isBinaryMsg;
        if (this.isBinaryMsg) {
            this.logger.info("[RAFT] Using Binary-based Protocol");
        } else {
            this.logger.info("[RAFT] Using Text-based Protocol");
        }

        this.logList.add(new LogEntry(0, 0, "NO_OP"));
    }

    public synchronized void start() {
        for (String node : clusterNodes) {
            if (node.contains(this.nodeId)) {
                int port = Integer.parseInt(node.split(":")[1]);
                this.listen(port);
                break;
            }
        }
        resetElectionTimeout();

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    public synchronized NodeState getState() {
        return this.state;
    }

    public synchronized String getLeader() {
        return this.currentLeader;
    }

    public synchronized String getNodeId() {
        return this.nodeId;
    }

    public synchronized int getCurrentTerm() {
        return this.currentTerm;
    }

    public synchronized void resetElectionTimeout() {
        if (electionTask != null && !electionTask.isCancelled()) {
            electionTask.cancel(true);
        }
        var electionTimeout = 1000 + ThreadLocalRandom.current().nextInt(1000);
        electionTask = timerExecutor.schedule(() -> {
            Thread.startVirtualThread(this::startElection);
        }, electionTimeout, TimeUnit.MILLISECONDS);
    }

    private void startElection() {
        int termToVote;
        int quorum = (clusterNodes.size() / 2) + 1;
        AtomicInteger grantedVotes = new AtomicInteger(1);

        synchronized (this) {
            if (this.state == NodeState.LEADER) return;

            this.state = NodeState.CANDIDATE;
            this.currentTerm++;
            this.votedFor = this.nodeId;
            termToVote = this.currentTerm;

            stateMachine.onBecomeCandidate();
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

        synchronized (this) {
            if (this.state == NodeState.CANDIDATE) {
                resetElectionTimeout();
            }
        }
    }

    private synchronized void checkElectionResult(int currentVotes, int quorum) {
        if (this.state == NodeState.CANDIDATE && currentVotes >= quorum) {
            this.state = NodeState.LEADER;
            if (electionTask != null) electionTask.cancel(true);
            // Khởi tạo các chỉ số quản lý Followers khi lên làm Leader
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
        synchronized (this) {
            termToSend = this.currentTerm;
            currentState = this.state;
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

        // Chụp ảnh trạng thái log hiện tại dành riêng cho Follower này
        int nextIdx = nextIndex.getOrDefault(targetNode, 1);
        prevLogIndex = nextIdx - 1;
        prevLogTerm = logList.get(prevLogIndex).term();
        leaderCommit = this.commitIndex;

        // Nếu Leader có log mới hơn mốc nextIndex của Follower -> Đóng gói gửi đi
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

        // Bắn gói tin vật lý TCP sang Follower
        String[] parts = targetNode.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            try (PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                // Gửi chuỗi đặc tả đầy đủ theo chuẩn Raft Paper
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

                        // Cập nhật chỉ số tiến độ của Follower dựa trên phản hồi
                        if (success) {
                            // Đồng bộ thành công: Cập nhật vị trí log khớp của Follower đó
                            matchIndex.put(targetNode, followerMatchIndex);
                            nextIndex.put(targetNode, followerMatchIndex + 1);

                            // Kiểm tra xem có Log Index nào đã được ĐA SỐ xác nhận chưa để tăng commitIndex
                            checkAndAdvanceCommitIndex();
                        } else {
                            // Đồng bộ thất bại (Lệch Log): Lùi lịch sử nextIndex về 1 nấc để dò lại đoạn khớp ở vòng sau
                            nextIndex.put(targetNode, Math.max(1, nextIdx - 1));
                        }
                    }
                }
            }
        } catch (Exception ex) {
            this.logger.error("[RAFT] Error when sync Logs with [Target-Node={}], [Term={}]: message={}",
                    targetNode, termToSend, ex.getMessage());
        }
    }

    private void syncLogWithFollowerBinary(String targetNode, int termToSend) {
        int prevLogIndex;
        int prevLogTerm;
        int leaderCommit;
        String entriesData = "";

        // Chụp ảnh trạng thái log hiện tại dành riêng cho Follower này
        int nextIdx = nextIndex.getOrDefault(targetNode, 1);
        prevLogIndex = nextIdx - 1;
        prevLogTerm = logList.get(prevLogIndex).term();
        leaderCommit = this.commitIndex;

        // Nếu Leader có log mới hơn mốc nextIndex của Follower -> Đóng gói gửi đi
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

        // Bắn gói tin vật lý TCP sang Follower
        String[] parts = targetNode.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            socket.setSoTimeout(200);
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 2048));
                 DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 2048))) {
                // Send request APPEND_ENTRIES
                // Write magic byte 'L'
                out.writeByte(0x4C);
                out.writeInt(termToSend);
                out.writeUTF(nodeId);
                out.writeInt(prevLogIndex);
                out.writeInt(prevLogTerm);
                out.writeInt(leaderCommit);

                // send data compress
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

                // handle response APPEND_REPLY
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

                    // Cập nhật chỉ số tiến độ của Follower dựa trên phản hồi
                    if (success) {
                        // Đồng bộ thành công: Cập nhật vị trí log khớp của Follower đó
                        matchIndex.put(targetNode, followerMatchIndex);
                        nextIndex.put(targetNode, followerMatchIndex + 1);

                        // Kiểm tra xem có Log Index nào đã được ĐA SỐ xác nhận chưa để tăng commitIndex
                        checkAndAdvanceCommitIndex();
                    } else {
                        // Đồng bộ thất bại (Lệch Log): Lùi lịch sử nextIndex về 1 nấc để dò lại đoạn khớp ở vòng sau
                        nextIndex.put(targetNode, Math.max(1, nextIdx - 1));
                    }
                }
                out.flush();
            }
        } catch (Exception ex) {
            this.logger.error("[RAFT] Error when sync Logs with [Target-Node={}], [Term={}]: message={}",
                    targetNode, termToSend, ex.getMessage());
        }
    }

    public void propose(String command) {
        int entryIndex;
        synchronized (this) {
            if (this.state != NodeState.LEADER) return;

            // Ghi lệnh mới vào Log cục bộ của Leader
            entryIndex = logList.size();
            logList.add(new LogEntry(entryIndex, this.currentTerm, command));
        }
    }

    private synchronized void checkAndAdvanceCommitIndex() {
        int quorum = (clusterNodes.size() / 2) + 1;
        int lastLogIndex = logList.size() - 1;

        // Duyệt từ commitIndex hiện tại tiến dần lên cuối mảng log
        for (int index = commitIndex + 1; index <= lastLogIndex; index++) {
            if (logList.get(index).term() == currentTerm) {
                int count = 1; // Bản thân Leader đã có
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
                        // Kích hoạt giải phóng lệnh propose(), trả về true lập tức cho Client
                        future.complete(true);
                    }
                }
            }
        }
    }

    private synchronized void applyLogsToStateMachine() {
        while (commitIndex > lastApplied) {
            lastApplied++;
            LogEntry entry = logList.get(lastApplied);
            if (this.stateMachine != null && !"NO_OP".equals(entry.command())) {
                stateMachine.onLogCommitted(entry.command());
            }
        }
    }

    public synchronized boolean handleAppendEntries(int leaderTerm, String leaderId, int prevLogIndex, int prevLogTerm, int leaderCommit, String entriesData) {
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

        // Kiểm tra tính nhất quán: Log của Follower có chứa entry tại
        // vị trí prevLogIndex có term trùng với prevLogTerm không?
        if (prevLogIndex >= logList.size() || logList.get(prevLogIndex).term() != prevLogTerm) {
            // Trả về false để bắt Leader lùi nextIndex lại dò tìm đoạn khớp lịch sử
            return false;
        }

        // Nếu khớp dữ liệu, tiến hành phân tích cú pháp và chèn các bản ghi mới từ Leader gửi sang
        if (entriesData != null && !entriesData.isBlank()) {
            String[] rawEntries = entriesData.split(";");
            int writeIndex = prevLogIndex + 1;

            for (String rawEntry : rawEntries) {
                if (rawEntry.isBlank()) continue;

                // Cắt theo dấu # với giới hạn là 3 phần tử
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
    }

    private boolean sendRequestVoteRpc(String targetNode, int term, String candidateId) {
        String[] parts = targetNode.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        try (Socket socket = new Socket()) {
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
            socket.connect(new InetSocketAddress(host, port), 1000);
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 512));
                 DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 512))) {
                // Send request vote: REQUEST_VOTE
                // Magic byte 'V'
                out.writeByte(0x56);
                out.writeInt(term);
                out.writeUTF(candidateId);
                out.flush();

                // handle response vote: VOTE_RESPONSE
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

    public synchronized void stepDownToFollower(int newerTerm) {
        if (newerTerm > this.currentTerm) {
            this.currentTerm = newerTerm;
            this.state = NodeState.FOLLOWER;
            this.votedFor = null;
            if (heartbeatTask != null) {
                heartbeatTask.cancel(true);
            }
            pendingProposals.forEach((index, future) -> {
                if (!future.isDone()) {
                    // Trả về thất bại
                    future.complete(false);
                }
            });
            pendingProposals.clear();
            resetElectionTimeout();
            stateMachine.onBecomeFollower();
        }
    }

    public synchronized boolean handleRequestVote(int candidateTerm, String candidateId) {
        if (candidateTerm < this.currentTerm) return false;
        if (candidateTerm > this.currentTerm) {
            this.currentTerm = candidateTerm;
            this.state = NodeState.FOLLOWER;
            this.votedFor = null;
            stateMachine.onBecomeFollower();
        }
        if (this.votedFor == null || this.votedFor.equals(candidateId)) {
            this.votedFor = candidateId;
            resetElectionTimeout();
            return true;
        }
        return false;
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

                boolean voteGranted = this.handleRequestVote(term, candidateId);
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
            socket.setSoTimeout(1000);
            byte magic = in.readByte();
            // check magic byte 'V' or 'L'
            if (magic == 0x56) {
                // Handle request vote: REQUEST_VOTE
                int term = in.readInt();
                String candidateId = in.readUTF();
                boolean voteGranted = this.handleRequestVote(term, candidateId);

                // Send response for vote: VOTE_RESPONSE
                out.writeByte(0x56);
                out.writeInt(this.currentTerm);
                out.writeBoolean(voteGranted);

                this.logger.info("[RAFT] Processed RequestVote from node [{}]: granted={}", candidateId, voteGranted);
            } else if (magic == 0x4C) {
                // Handle request log: APPEND_ENTRIES
                int leaderTerm = in.readInt();
                String leaderId = in.readUTF();
                int prevLogIndex = in.readInt();
                int prevLogTerm = in.readInt();
                int leaderCommit = in.readInt();
                int entriesDataLength = in.readInt();
                if (entriesDataLength < 0 || entriesDataLength > 10 * 1024 * 1024) {
                    // If logs size larger than 10MB -> fast fail
                    return;
                }
                // read compress data
                byte[] compressedBytes = new byte[entriesDataLength];
                in.readFully(compressedBytes);
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

                // send response for log: APPEND_REPLY
                out.writeByte(0x4C);
                out.writeInt(this.currentTerm);
                out.writeBoolean(success);
                out.writeInt((logList.size() - 1));
            }
            out.flush();
        } catch (Exception e) {
            this.logger.error("[RAFT] Error processing Raft RPC", e);
        }
    }

    public synchronized void stop() {
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
    }
}
