# Design: convert_equality_deletes

## Problem

Flink's Iceberg sink emits equality delete files for streaming mutations.
Each equality delete triggers a hash-join at read time — every scan must
match delete values against data rows. As mutation volume grows, the
accumulating eq-delete files degrade read performance proportionally.

Deletion vectors (DVs) store pre-resolved row positions as Roaring bitmaps
in Puffin files. Readers apply DVs via a simple bitmap lookup instead of
a join, eliminating the per-scan cost entirely.

No existing tool performs this conversion end-to-end via Spark. Amoro
resolves eq-deletes during data file rewrites — it reads eq-delete values
into an in-memory set, filters data rows at rewrite time, and drops both
the eq-deletes and the old data files. This eliminates the eq-deletes but
rewrites all affected data, which is expensive and keeps read amplification
high during the rewrite window. The `ConvertEqualityDeleteFiles` action
interface exists in Iceberg but had no Spark implementation.

## Architecture

### Per-partition processing

Each table partition is processed as an independent Spark job rather than
combining all partitions into a single monolithic join.

**Why:** A single job across all partitions creates an unbounded Spark plan.
For tables with thousands of partitions and millions of data files, this
produces a plan too large for the driver to optimize and execute. Per-partition
jobs keep each plan small and bounded. This follows the same pattern as
`RewriteDataFilesSparkAction`, which processes file groups independently
with configurable concurrency via `Tasks.foreach` and an `ExecutorService`.

**Tradeoff:** More Spark jobs means more scheduling overhead. For tables with
many small partitions, the per-job overhead may dominate. The
`max-concurrent-partitions` option controls how many run in parallel —
defaulting to 1 (sequential) for safety, tunable up for throughput.

### Parallel partition execution

Partitions are submitted to an `ExecutorService` thread pool using Iceberg's
`Tasks.foreach` pattern (same as `RewriteDataFilesSparkAction`). Each thread
submits an independent Spark job. The SparkContext is shared and thread-safe
for job submission.

**Why not Spark-level parallelism:** Spark's DataFrame API doesn't natively
support "run these N independent joins in parallel." The thread pool approach
gives explicit control over concurrency without requiring a custom Spark
scheduler.

### Per equality-schema-group joins

Within a partition, equality delete files may have different `equality_ids`
(e.g., one set deletes by `{id}`, another by `{name}`). Each distinct set
requires a separate join because the projected columns differ.

The outputs from all groups are unioned and deduplicated before DV writing.
Deduplication handles the case where two groups match the same data row
(e.g., `id=1` and `name='foo'` both refer to the same row).

### File reading via FormatModelRegistry

Files are read through Iceberg's `FormatModelRegistry`, not Spark's default
`spark.read().parquet()`.

**Why:** Spark's Parquet reader matches columns by name. Iceberg's reader
matches by field ID, handling column renames and schema evolution
transparently. A table where column `user_name` was renamed to `name`
would silently produce wrong results with Spark's reader but works
correctly with Iceberg's field-ID matching.

**Format support:** The same `ReadBuilder` interface supports Parquet, ORC,
and Avro. Parquet and ORC use vectorized reads (`ColumnarBatch`) when all
projected columns are primitive — we prefer this path to keep data file
scans as fast as possible, since data files are the dominant I/O cost. Avro
falls back to row-by-row (`InternalRow`). The format is determined per-file
from the inventory metadata.

**Tradeoff:** `FormatModelRegistry` reads produce `InternalRow` or
`ColumnarBatch`, not Spark `Row`. We convert via
`CatalystTypeConverters.convertToScala` in lazy streaming iterators
(`BatchRowIterator`, `InternalRowIterator`). This adds per-row conversion
overhead that `spark.read().parquet()` avoids — but correctness across
schema evolution and format diversity is worth the cost.

### Sequence number filtering

Iceberg's equality delete semantics require that a delete only applies to
data written before it (lower sequence number). The join condition includes
`data_sequence_num < eq_sequence_num` alongside the equality column match.

Sequence numbers are collected from the partition-scoped ENTRIES metadata
table, not the full table. Each file is tagged with its sequence number as
a literal column during the read, avoiding path-format mismatches between
`_metadata.file_path` and Iceberg manifest paths.

### Row position via MetadataColumns.ROW_POSITION

The `_pos` metadata column is included in the data file projection schema.
Both the vectorized and row-by-row readers auto-populate it with the row's
ordinal position within the file. No manual counter is needed.

**Why not `_metadata.row_index`:** That's a Spark-specific virtual column
available only through `spark.read().parquet()`. Since we read through
FormatModelRegistry, we use Iceberg's `_pos` which is handled by the reader
infrastructure directly.

### DV writing on executors with dummy partition spec

`BaseDVFileWriter` requires a `PartitionSpec` and `StructLike` partition for
each `delete()` call, but these are only used to build the `DeleteFile`
metadata — not the DV content itself. We pass `PartitionSpec.unpartitioned()`
and `null` on executors, then reconstruct correct partition metadata on the
driver from the inventory data.

**Why:** `PartitionSpec` and `StructLike` are not cleanly serializable to
executors. Rather than building serialization infrastructure for partition
data, we let the driver (which already has the `DataFile` objects with
correct partition info) reconstruct the `DeleteFile` metadata.

### Atomic commit with validateFromSnapshot

All DVs from all partitions are committed in a single `RewriteFiles`
operation with `validateFromSnapshot`. This ensures the commit fails if the
table changed since the inventory was taken — a safe optimistic concurrency
check. On failure, the job can be re-run.

This single-commit design also means we must coalesce all DV writes for a
given data file into one task. If the same data file were written to by
separate partition or equality-schema-group tasks, the commit would add
multiple DVs for one data file — valid but suboptimal. The per-partition
architecture prevents this: within a partition, all equality-schema groups
produce positions that are unioned, deduplicated, and written together.
Since a data file belongs to exactly one partition, no cross-partition
conflicts occur.

**Tradeoff:** No partial progress — if one partition fails, none commit.
For very large tables, partition-by-partition commits (like
`RewriteDataFilesSparkAction`'s `partial-progress.enabled`) would be safer.
This is a future enhancement.

### Filter ordering

The user's filter expression is applied to the eq-delete file list *before*
the `max-partitions` limit. This prevents the limit from excluding the
filtered partition. Without this ordering, a user filtering on `category =
'electronics'` with `max-partitions = 1` might get a different partition
(the heaviest by eq-delete count) instead of the intended one.

### Existing DV merge

When a data file already has a DV (from a prior compaction or conversion),
`BaseDVFileWriter.loadPreviousDeletes` reads the existing DV bytes and
merges them with the new positions. The old DV is removed and a new merged
DV is added in the commit.

This ensures exactly one DV per data file at all times. The merge itself
is fast — Roaring bitmaps support O(n) union operations on compressed
sorted integer sets without decompressing individual positions.

The existing DV is deserialized using `PositionDeleteIndex.deserialize(bytes,
deleteFile)`, which validates bitmap length and cardinality against the
`DeleteFile` metadata. We construct a minimal `DeleteFile` on the executor
from broadcast `DvInfo` (location, offset, size, record count, referenced
data file) to satisfy this validation.

## Configuration

| Option | Default | Rationale |
|--------|---------|-----------|
| `max-partitions` | unlimited | Caps driver memory pressure from inventory collection. Heaviest partitions (most eq-deletes) are prioritized. |
| `max-concurrent-partitions` | 1 | Controls cluster resource usage. Higher values improve throughput but increase concurrent Spark job load. |
| `join-strategy` | `auto` | Lets Spark's CBO choose. `broadcast` is best when eq-deletes are small; `hash`/`sort-merge` for larger ones. |

## Limitations

- **Format version 3 required.** DVs are a v3 feature.
- **No partial progress commits.** All-or-nothing per run.
- **Data files read N times per partition** (once per equality-schema group).
  Equality column projection limits I/O but the overhead is proportional to
  the number of distinct equality schemas.
- **Orphan Puffin files on failure.** Written DVs are not cleaned up if the
  commit fails. Recoverable via `remove_orphan_files`.
