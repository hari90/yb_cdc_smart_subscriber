## Yugabyte CDC Smart Subscriber

This Java app streams logical changes from a source YugabyteDB cluster using `wal2json` and applies them to a sink cluster. It filters out transactions by `origin`/`origin_id` so changes originally written by the sink are not re-applied (prevents loops).

### Build

Requirements: Java 11+, Maven 3.8+

```bash
mvn -q -DskipTests package
```

Produces a fat jar:

```
target/cdc-smart-subscriber-0.1.0-shaded.jar
```

### Prerequisites

On each cluster (run as a superuser):

```sql
-- Create the yb_origin table to track origin ids and break loops
CREATE TABLE yb_origin(id uuid DEFAULT gen_random_uuid(), origin_id TEXT, PRIMARY KEY(id HASH)) SPLIT INTO 10 TABLETS;

-- Temporary workaround
SET yb_non_ddl_txn_for_sys_tables_allowed = TRUE;
-- Set the origin name for each of the remote clusters
SELECT pg_replication_origin_create('<remote_cluster_name>');
-- Create the replication slot for the remote cluster
SELECT pg_create_logical_replication_slot('<db_name>_<local_cluster_name>_<remote_cluster_name>', 'wal2json');
```

### Origin filtering/stamping

The app:
- Skips the transactions where the origin id is the same as the sink origin id.
- Since yb does not yet support `origin_id` in wal2json, it injects the origin id into the sink table `yb_origin` and on the receiving end, it looks up the origin name from the `origin_id`.

### Run

```bash
java -jar target/cdc-smart-subscriber-0.1.0-shaded.jar --source-uri=<sourceUri> --source-origin-name=<sourceOriginName> --sink-uri=<sinkUri> --sink-origin-name=<sinkOriginName> --source-replication-slot=<replicationSlot> --user-name=<userName> --password=<password> [--verbose] [--startlsn=<startlsn>]
```

You must pass the source URI, sink URI, and sink origin name via CLI

```bash
java -jar target/yb-cdc-smart-subscriber-0.1.0.jar \
  --source-uri=jdbc:postgresql://cluster_a_url:5433/db1 \
  --source-origin-name=cluster_a \
  --sink-uri=jdbc:postgresql://cluster_b_url:5433/db1 \
  --sink-origin-name=cluster_b \
  --source-replication-slot=slot_db1_cluster_a_cluster_b \
  --user-name=user \
  --password=password
```
### Notes
- Requires primary keys or replica identity for correct updates/deletes (wal2json uses `oldkeys`, `pk`).
- YugabyteDB support for logical decoding / replication origin functions may vary by version; ensure they are available in your deployment.
- For bi-directional setups, ensure consistent `SINK_ORIGIN_NAME` across writers so filtering works reliably.
