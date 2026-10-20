package lab.retry;

import java.time.Duration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

final class FlussStore implements AutoCloseable {
    private static final TablePath PATH = TablePath.of("retry_lab", "job_events");
    private final Connection connection;
    private final Admin admin;
    private final int timeout;
    private final String bootstrap;
    private final TablePath path;
    private final String lifetime = UUID.randomUUID().toString();
    private final Thread owner = Thread.currentThread();
    private Table activeTable;
    private TableInfo openedInfo;
    private AppendWriter writer;
    private boolean closed;

    FlussStore(String bootstrap, int timeout) throws Exception {
        this(bootstrap, timeout, PATH);
    }

    private FlussStore(String bootstrap, int timeout, TablePath path) throws Exception {
        this.bootstrap = bootstrap;
        this.path = path;
        this.timeout = timeout;
        Configuration config = new Configuration();
        config.setString("bootstrap.servers", bootstrap);
        config.setString("client.writer.acks", "all");
        config.setString("client.writer.retries", "1");
        config.setString("client.request-timeout", "10s");
        config.setString("client.connect-timeout", "10s");
        config.setString("client.writer.buffer.wait-timeout", "10s");
        connection = ConnectionFactory.createConnection(config);
        try {
            admin = connection.getAdmin();
        } catch (Exception failure) {
            closeAfterFailure(connection, failure);
            throw failure;
        }
    }

    private static Schema schema() {
        return Schema.newBuilder().column("job_id", DataTypes.STRING())
                .column("event_id", DataTypes.STRING()).column("attempt", DataTypes.INT())
                .column("step", DataTypes.INT()).column("event_type", DataTypes.STRING())
                .column("action_id", DataTypes.STRING()).column("payload_digest", DataTypes.STRING())
                .column("emitted_at_ms", DataTypes.BIGINT()).column("detail", DataTypes.STRING()).build();
    }

    private void requireOwner() {
        require(Thread.currentThread() == owner, "Store is thread-confined");
        require(!closed, "Store is closed");
    }

    private Table table() throws Exception {
        requireOwner();
        if (activeTable != null) {
            return activeTable;
        }
        Table candidate = connection.getTable(path);
        try {
            TableInfo info = candidate.getTableInfo();
            validateTable(info);
            Json.emit("TABLE_OPEN", Map.of("store_lifetime", lifetime, "table_id", info.getTableId()));
            openedInfo = info;
            activeTable = candidate;
            return activeTable;
        } catch (Exception failure) {
            closeAfterFailure(candidate, failure);
            throw failure;
        }
    }

    private static void validateTable(TableInfo info) {
        require(info.getRowType().equals(schema().getRowType()), "Schema mismatch");
        require(!info.hasPrimaryKey(), "Expected append Log Table without primary key");
        require(!info.isPartitioned(), "Expected nonpartitioned table");
        require(info.getNumBuckets() == 1, "Expected one bucket");
        require(info.getBucketKeys().equals(List.of("job_id")), "Expected job_id bucket key");
        require("0ms".equals(info.getProperties().toMap().get("table.log.ttl")), "Expected disabled log TTL");
    }

    private void verifyUnchanged() throws Exception {
        TableInfo current = admin.getTableInfo(path).get(timeout, TimeUnit.SECONDS);
        validateTable(current);
        require(current.equals(openedInfo), "Table metadata changed during store lifetime");
    }

    String lifetime() {
        return lifetime;
    }

    void ready() throws Exception {
        admin.listDatabases().get(timeout, TimeUnit.SECONDS);
        Json.emit("RESULT", Map.of("state", "READY"));
    }

    void initialize() throws Exception {
        admin.createDatabase("retry_lab", DatabaseDescriptor.builder().build(), true)
                .get(timeout, TimeUnit.SECONDS);
        TableDescriptor descriptor = TableDescriptor.builder().schema(schema())
                .distributedBy(1, "job_id").property("table.log.ttl", "0ms").property("table.replication.factor", "1").build();
        admin.createTable(path, descriptor, false).get(timeout, TimeUnit.SECONDS);
        table();
        require(offset(new OffsetSpec.EarliestSpec()) == 0, "Fresh table earliest must be 0");
        require(offset(new OffsetSpec.LatestSpec()) == 0, "Fresh table latest must be 0");
        append(Event.probe(1));
        require(offset(new OffsetSpec.LatestSpec()) == 1, "First append latest must be exclusive 1");
        History.Snapshot first = scanTo(1);
        require(first.records().get(0).event().equals(Event.probe(1)), "First probe read differs");
        append(Event.probe(2));
        require(offset(new OffsetSpec.LatestSpec()) == 2, "Second append latest must be exclusive 2");
        History.Snapshot second = scanTo(2);
        validateProbe(second);
        Json.emit("RESULT", Map.of("state", "INITIALIZED", "probe", second,
                "exclusive_end_verified", true, "client_writer_acks", "all", "log_ttl", "0ms",
                "table_id", openedInfo.getTableId(), "table_properties", openedInfo.getProperties().toMap()));
    }

    void append(Event event) throws Exception {
        Table table = table();
        if (writer == null) {
            writer = table.newAppend().createWriter();
            Json.emit("WRITER_OPEN", Map.of("store_lifetime", lifetime));
        }
        CompletableFuture<AppendResult> appendFuture = writer.append(event.row());
        writer.flush();
        appendFuture.get(timeout, TimeUnit.SECONDS);
        Json.emit("ACK", Map.of("event_id", event.event_id(), "event_type", event.event_type(),
                "attempt", event.attempt(), "pid", ProcessHandle.current().pid(), "store_lifetime", lifetime));
    }

    private long offset(OffsetSpec spec) throws Exception {
        return admin.listOffsets(path, Collections.singletonList(0), spec)
                .bucketResult(0).get(timeout, TimeUnit.SECONDS);
    }

    History.Snapshot snapshot() throws Exception {
        return snapshot(null);
    }

    History.Snapshot snapshot(Path release) throws Exception {
        table();
        verifyUnchanged();
        long end = offset(new OffsetSpec.LatestSpec());
        require(end >= 2, "Initial exclusive-end probe missing; run init on a fresh table");
        if (release != null) {
            awaitRelease(release, end);
        }
        History.Snapshot snapshot = scanTo(end);
        validateProbe(snapshot);
        return snapshot;
    }

    private History.Snapshot scanTo(long end) throws Exception {
        table();
        verifyUnchanged();
        require(offset(new OffsetSpec.EarliestSpec()) == 0, "Retention gap: earliest is not zero");
        List<History.Raw> records = new ArrayList<>();
        long progress = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeout);
        Table activeTable = table();
        TableBucket expectedBucket = new TableBucket(activeTable.getTableInfo().getTableId(), 0);
        try (LogScanner scanner = activeTable.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            while (progress < end) {
                require(System.nanoTime() < deadline, "Bounded replay timed out before captured latest");
                ScanRecords batch = scanner.poll(Duration.ofMillis(250));
                collect(batch, records, end);
                progress = consumedProgress(batch, expectedBucket, progress, end, records.size());
            }
        }
        verifyUnchanged();
        require(records.size() == end, "Required raw prefix missing despite exclusive scanner progress");
        require(offset(new OffsetSpec.LatestSpec()) == end, "Writer active during recovery snapshot");
        require(offset(new OffsetSpec.EarliestSpec()) == 0, "Retention changed during replay");
        return new History.Snapshot(end, List.copyOf(records));
    }

    private static long consumedProgress(ScanRecords batch, TableBucket expectedBucket,
                                         long progress, long end, long rawCount) {
        require(batch.buckets().size() <= 1, "Unexpected buckets in one-bucket replay");
        for (TableBucket bucket : batch.buckets()) {
            require(bucket.equals(expectedBucket), "Unexpected table bucket in replay");
            Long next = batch.consumedUpToOffset(bucket);
            require(next != null, "Missing exclusive scanner progress for polled bucket");
            require(next >= progress, "Exclusive scanner progress regressed");
            require(next <= end, "Exclusive scanner progress crossed captured latest");
            require(next >= rawCount, "Exclusive scanner progress precedes observed raw rows");
            progress = next;
        }
        return progress;
    }

    private static void collect(ScanRecords batch, List<History.Raw> records, long end) {
        for (ScanRecord record : batch) {
            require(record.logOffset() < end, "Read crossed captured latest; writer was not stopped");
            require(record.logOffset() == records.size(), "Missing prefix or noncontiguous log offset");
            records.add(new History.Raw(0, record.logOffset(), Event.from(record.getRow())));
        }
    }

    private static void validateProbe(History.Snapshot snapshot) throws Exception {
        require(snapshot.records().size() >= 2, "Offset probe prefix missing");
        require(snapshot.records().get(0).event().equals(Event.probe(1)), "Probe first row mismatch");
        require(snapshot.records().get(1).event().equals(Event.probe(2)), "Probe second row mismatch");
    }

    static void require(boolean condition, String error) {
        if (!condition) {
            throw new IllegalStateException(error);
        }
    }

    private void awaitRelease(Path release, long end) throws Exception {
        require(!Files.exists(release), "Snapshot release file already exists");
        Json.emit("BARRIER", Map.of("barrier", "SNAPSHOT", "end_offset_exclusive", end,
                "pid", ProcessHandle.current().pid()));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeout);
        while (!Files.exists(release)) {
            require(System.nanoTime() < deadline, "Snapshot release timed out");
            Thread.sleep(50);
        }
    }

    void metadataGuard(String kind) throws Exception {
        TablePath negativePath = TablePath.of("retry_lab", "guard_" + kind);
        Schema selected = kind.equals("schema")
                ? Schema.newBuilder().column("wrong", DataTypes.STRING()).build() : schema();
        TableDescriptor descriptor = TableDescriptor.builder().schema(selected)
                .distributedBy(1, kind.equals("schema") ? "wrong" : "job_id")
                .property("table.log.ttl", "0ms").property("table.replication.factor", "1").build();
        admin.createTable(negativePath, descriptor, false).get(timeout, TimeUnit.SECONDS);
        try (FlussStore candidate = new FlussStore(bootstrap, timeout, negativePath)) {
            if (kind.equals("mutation")) {
                candidate.table();
                admin.dropTable(negativePath, false).get(timeout, TimeUnit.SECONDS);
                admin.createTable(negativePath, descriptor, false).get(timeout, TimeUnit.SECONDS);
                candidate.verifyUnchanged();
            } else {
                candidate.table();
            }
        }
        throw new IllegalStateException("Metadata guard unexpectedly accepted invalid table");
    }

    private static void closeAfterFailure(AutoCloseable resource, Exception failure) {
        try {
            resource.close();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static Exception closeResource(AutoCloseable resource, Exception failure) {
        if (resource == null) {
            return failure;
        }
        try {
            resource.close();
            return failure;
        } catch (Exception closeFailure) {
            if (failure == null) {
                return closeFailure;
            }
            failure.addSuppressed(closeFailure);
            return failure;
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        requireOwner();
        closed = true;
        Exception failure = closeResource(activeTable, null);
        failure = closeResource(admin, failure);
        failure = closeResource(connection, failure);
        if (failure != null) {
            throw failure;
        }
    }
}
