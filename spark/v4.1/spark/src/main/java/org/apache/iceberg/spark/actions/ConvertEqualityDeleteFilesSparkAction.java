/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.actions;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ConvertEqualityDeleteFiles;
import org.apache.iceberg.actions.ImmutableConvertEqualityDeleteFiles;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.SparkDeleteFile;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts equality delete files to deletion vectors (DVs) on a per-partition basis.
 *
 * <p>Each table partition is processed as an independent Spark job. Within a partition, each
 * equality-schema group (distinct set of equality field IDs) is joined separately against the
 * partition's data files, then the matched positions are unioned, deduplicated, and written as DVs.
 *
 * <p>Files are read using {@link FormatModelRegistry} (field-ID matching, vectorized for
 * Parquet/ORC, row-by-row for Avro).
 */
public class ConvertEqualityDeleteFilesSparkAction
    extends BaseSnapshotUpdateSparkAction<ConvertEqualityDeleteFilesSparkAction>
    implements ConvertEqualityDeleteFiles {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConvertEqualityDeleteFilesSparkAction.class);

  static final String MAX_PARTITIONS = "max-partitions";
  static final String MAX_CONCURRENT_PARTITIONS = "max-concurrent-partitions";
  static final String JOIN_STRATEGY = "join-strategy";
  static final String JOIN_STRATEGY_AUTO = "auto";
  static final String JOIN_STRATEGY_BROADCAST = "broadcast";
  static final String JOIN_STRATEGY_HASH = "hash";
  static final String JOIN_STRATEGY_SORT_MERGE = "sort-merge";

  private static final Set<FileFormat> VECTORIZABLE_FORMATS =
      ImmutableSet.of(FileFormat.PARQUET, FileFormat.ORC);

  private static final int DEFAULT_BATCH_SIZE = 4096;

  private static final Result EMPTY_RESULT =
      ImmutableConvertEqualityDeleteFiles.Result.builder()
          .convertedEqualityDeleteFilesCount(0)
          .addedPositionDeleteFilesCount(0)
          .addedDeletionVectorCount(0)
          .build();

  private static final StructType DV_RESULT_SCHEMA =
      new StructType()
          .add("referenced_data_file", DataTypes.StringType)
          .add("dv_path", DataTypes.StringType)
          .add("dv_file_size", DataTypes.LongType)
          .add("content_offset", DataTypes.LongType)
          .add("content_size", DataTypes.LongType)
          .add("record_count", DataTypes.LongType);

  private static final StructType FILE_LIST_SCHEMA =
      new StructType()
          .add("file_path", DataTypes.StringType)
          .add("file_format", DataTypes.StringType)
          .add("sequence_num", DataTypes.LongType);

  private final Table table;
  private Expression filter = Expressions.alwaysTrue();

  ConvertEqualityDeleteFilesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected ConvertEqualityDeleteFilesSparkAction self() {
    return this;
  }

  @Override
  public ConvertEqualityDeleteFilesSparkAction filter(Expression expression) {
    filter = Expressions.and(filter, expression);
    return this;
  }

  @Override
  public Result execute() {
    if (table.currentSnapshot() == null) {
      return EMPTY_RESULT;
    }

    String desc = String.format("Converting equality deletes to DVs in %s", table.name());
    JobGroupInfo info = newJobGroupInfo("CONVERT-EQ-DELETES", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  // --- Orchestration ---

  private Result doExecute() {
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    Types.StructType combinedFileType = DataFile.getType(Partitioning.partitionType(table));

    // Step 1: Inventory — collect eq-deletes, apply filter, then limit partitions
    Dataset<Row> entries = loadMetadataTable(table, MetadataTableType.ENTRIES).filter("status < 2");

    List<DeleteFile> eqDeleteFiles =
        collectDeleteFiles(
            entries.filter("data_file.content = 2").select("data_file.*"), combinedFileType);

    if (!Expressions.alwaysTrue().equals(filter)) {
      eqDeleteFiles = filterByExpression(eqDeleteFiles);
    }

    if (eqDeleteFiles.isEmpty()) {
      LOG.info("No equality delete files found in {}", table.name());
      return EMPTY_RESULT;
    }

    eqDeleteFiles = applyPartitionLimit(eqDeleteFiles);
    LOG.info("Processing {} equality delete files in {}", eqDeleteFiles.size(), table.name());

    // Build partition-filtered entries for data files and DVs
    boolean unpartitioned = table.spec().isUnpartitioned();
    Dataset<Row> scopedEntries = entries;
    if (!unpartitioned) {
      Dataset<Row> affectedPartitions = buildAffectedPartitionsDF(entries, eqDeleteFiles);
      scopedEntries = filterByPartitions(entries, affectedPartitions);
    }

    // Collect sequence numbers, data files, and DVs from partition-scoped entries
    Map<String, Long> eqSequenceNums = collectSequenceNums(scopedEntries, eqDeleteFiles);
    Map<String, DataFile> dataFilesByPath =
        collectDataFiles(scopedEntries.filter("data_file.content = 0"), combinedFileType);
    Map<String, Long> dataSequenceNums =
        collectSequenceNumsForDataFiles(scopedEntries, dataFilesByPath);
    Map<String, DeleteFile> existingDvsByDataFile =
        collectExistingDvs(
            scopedEntries.filter(
                "data_file.content = 1 AND UPPER(data_file.file_format) = 'PUFFIN'"),
            combinedFileType);

    // Step 2: Group by partition, process each in parallel
    Map<String, PartitionWorkUnit> workUnits =
        buildWorkUnits(
            eqDeleteFiles,
            dataFilesByPath,
            existingDvsByDataFile,
            eqSequenceNums,
            dataSequenceNums);

    Broadcast<Table> tableBroadcast =
        sparkContext().broadcast(SerializableTableWithSize.copyOf(table));
    Map<String, DvInfo> allDvInfo = buildDvInfoMap(existingDvsByDataFile);
    Broadcast<Map<String, DvInfo>> dvMapBroadcast = sparkContext().broadcast(allDvInfo);

    ConcurrentLinkedQueue<List<Row>> allDvResults = new ConcurrentLinkedQueue<>();
    int concurrency = PropertyUtil.propertyAsInt(options(), MAX_CONCURRENT_PARTITIONS, 1);
    ExecutorService service = partitionExecutorService(concurrency);

    try {
      Tasks.foreach(workUnits.values())
          .executeWith(service)
          .stopOnFailure()
          .noRetry()
          .run(
              workUnit -> {
                List<Row> result = processPartition(workUnit, tableBroadcast, dvMapBroadcast);
                if (!result.isEmpty()) {
                  allDvResults.add(result);
                }
              });
    } finally {
      service.shutdown();
    }

    List<Row> flatResults =
        allDvResults.stream().flatMap(List::stream).collect(Collectors.toList());
    if (flatResults.isEmpty()) {
      return EMPTY_RESULT;
    }

    // Step 3: Commit atomically
    List<DeleteFile> newDvs = buildDvDeleteFiles(flatResults, dataFilesByPath);
    commitConversion(startingSnapshotId, eqDeleteFiles, existingDvsByDataFile, newDvs);

    LOG.info(
        "Converted {} equality delete files to {} DVs in {}",
        eqDeleteFiles.size(),
        newDvs.size(),
        table.name());

    return ImmutableConvertEqualityDeleteFiles.Result.builder()
        .convertedEqualityDeleteFilesCount(eqDeleteFiles.size())
        .addedPositionDeleteFilesCount(0)
        .addedDeletionVectorCount(newDvs.size())
        .build();
  }

  /** Processes one table partition: join per equality-schema group, dedup, write DVs. */
  private List<Row> processPartition(
      PartitionWorkUnit workUnit,
      Broadcast<Table> tableBroadcast,
      Broadcast<Map<String, DvInfo>> dvMapBroadcast) {

    Map<List<Integer>, List<DeleteFile>> eqDeletesByFieldIds =
        workUnit.eqDeleteFiles.stream()
            .collect(
                Collectors.groupingBy(
                    df -> {
                      List<Integer> ids = new ArrayList<>(df.equalityFieldIds());
                      Collections.sort(ids);
                      return ids;
                    }));

    Dataset<Row> allPositions = null;

    for (Map.Entry<List<Integer>, List<DeleteFile>> group : eqDeletesByFieldIds.entrySet()) {
      List<String> equalityColumnNames =
          group.getKey().stream()
              .map(id -> table.schema().findColumnName(id))
              .collect(Collectors.toList());

      Dataset<Row> positions =
          resolvePositions(group.getValue(), equalityColumnNames, workUnit, tableBroadcast);
      allPositions = (allPositions == null) ? positions : allPositions.union(positions);
    }

    if (allPositions == null) {
      return Collections.emptyList();
    }

    return allPositions
        .dropDuplicates("data_file_path", "row_pos")
        .repartition(col("data_file_path"))
        .sortWithinPartitions("data_file_path", "row_pos")
        .mapPartitions(new WriteDVs(tableBroadcast, dvMapBroadcast), Encoders.row(DV_RESULT_SCHEMA))
        .collectAsList();
  }

  // --- Position Resolution ---

  private Dataset<Row> resolvePositions(
      List<DeleteFile> eqDeleteGroup,
      List<String> equalityColumnNames,
      PartitionWorkUnit workUnit,
      Broadcast<Table> tableBroadcast) {

    Schema equalitySchema = table.schema().select(equalityColumnNames);
    StructType eqSparkSchema = SparkSchemaUtil.convert(equalitySchema);

    StructType eqOutputSchema = eqSparkSchema.add("eq_sequence_num", DataTypes.LongType);
    Schema dataReadSchema =
        new Schema(appendField(equalitySchema.columns(), MetadataColumns.ROW_POSITION));
    StructType dataOutputSchema =
        eqSparkSchema
            .add("row_pos", DataTypes.LongType)
            .add("data_file_path", DataTypes.StringType)
            .add("data_sequence_num", DataTypes.LongType);

    // Read eq-delete files
    Dataset<Row> eqDeletes =
        createFileListDF(eqDeleteGroup, workUnit.eqSequenceNums)
            .flatMap(
                new IcebergFileReader(tableBroadcast, equalitySchema, eqSparkSchema, false),
                Encoders.row(eqOutputSchema));

    // Read data files
    List<Row> dataFileRows = new ArrayList<>();
    for (Map.Entry<String, DataFile> entry : workUnit.dataFiles.entrySet()) {
      Long seq = workUnit.dataSequenceNums.get(entry.getKey());
      if (seq != null) {
        dataFileRows.add(RowFactory.create(entry.getKey(), entry.getValue().format().name(), seq));
      }
    }

    Dataset<Row> dataRows =
        spark()
            .createDataFrame(dataFileRows, FILE_LIST_SCHEMA)
            .flatMap(
                new IcebergFileReader(tableBroadcast, dataReadSchema, eqSparkSchema, true),
                Encoders.row(dataOutputSchema));

    Dataset<Row> eqDeletesHinted = applyJoinStrategy(eqDeletes);
    Column joinCond = col("data_sequence_num").$less(col("eq_sequence_num"));
    for (String eqCol : equalityColumnNames) {
      joinCond = joinCond.and(dataRows.col(eqCol).eqNullSafe(eqDeletesHinted.col(eqCol)));
    }

    return dataRows.join(eqDeletesHinted, joinCond, "leftsemi").select("data_file_path", "row_pos");
  }

  // --- Inventory ---

  private List<DeleteFile> filterByExpression(List<DeleteFile> deleteFiles) {
    Map<Integer, Evaluator> evaluatorsBySpec = new HashMap<>();
    for (Map.Entry<Integer, PartitionSpec> entry : table.specs().entrySet()) {
      Expression projected = Projections.inclusive(entry.getValue()).project(filter);
      evaluatorsBySpec.put(
          entry.getKey(), new Evaluator(entry.getValue().partitionType(), projected));
    }

    return deleteFiles.stream()
        .filter(
            df -> {
              Evaluator eval = evaluatorsBySpec.get(df.specId());
              return eval != null && eval.eval(df.partition());
            })
        .collect(Collectors.toList());
  }

  private List<DeleteFile> applyPartitionLimit(List<DeleteFile> eqDeleteFiles) {
    int maxPartitions = PropertyUtil.propertyAsInt(options(), MAX_PARTITIONS, Integer.MAX_VALUE);
    if (maxPartitions >= Integer.MAX_VALUE) {
      return eqDeleteFiles;
    }

    Map<String, List<DeleteFile>> byPartition = groupByPartitionKey(eqDeleteFiles);
    List<String> orderedKeys =
        byPartition.entrySet().stream()
            .sorted((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()))
            .limit(maxPartitions)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

    return orderedKeys.stream()
        .flatMap(key -> byPartition.get(key).stream())
        .collect(Collectors.toList());
  }

  private Map<String, Long> collectSequenceNums(
      Dataset<Row> scopedEntries, List<DeleteFile> files) {
    Set<String> paths = files.stream().map(DeleteFile::location).collect(Collectors.toSet());
    Map<String, Long> map = new HashMap<>();
    scopedEntries
        .filter("data_file.content = 2")
        .selectExpr("data_file.file_path", "sequence_number")
        .collectAsList()
        .forEach(
            r -> {
              String path = r.getString(0);
              if (paths.contains(path)) {
                map.put(path, r.getLong(1));
              }
            });
    return map;
  }

  private Map<String, Long> collectSequenceNumsForDataFiles(
      Dataset<Row> scopedEntries, Map<String, DataFile> dataFiles) {
    Map<String, Long> map = new HashMap<>();
    scopedEntries
        .filter("data_file.content = 0")
        .selectExpr("data_file.file_path", "sequence_number")
        .collectAsList()
        .forEach(
            r -> {
              String path = r.getString(0);
              if (dataFiles.containsKey(path)) {
                map.put(path, r.getLong(1));
              }
            });
    return map;
  }

  private Dataset<Row> buildAffectedPartitionsDF(
      Dataset<Row> entries, List<DeleteFile> eqDeleteFiles) {
    Set<String> eqPaths =
        eqDeleteFiles.stream().map(DeleteFile::location).collect(Collectors.toSet());
    return entries
        .filter("data_file.content = 2")
        .filter(col("data_file.file_path").isInCollection(eqPaths))
        .selectExpr("data_file.partition AS partition", "data_file.spec_id AS spec_id")
        .distinct();
  }

  private Dataset<Row> filterByPartitions(Dataset<Row> entries, Dataset<Row> affectedPartitions) {
    return entries.join(
        affectedPartitions,
        entries
            .col("data_file.partition")
            .equalTo(affectedPartitions.col("partition"))
            .and(entries.col("data_file.spec_id").equalTo(affectedPartitions.col("spec_id"))),
        "leftsemi");
  }

  // --- Grouping helpers ---

  private Map<String, PartitionWorkUnit> buildWorkUnits(
      List<DeleteFile> eqDeleteFiles,
      Map<String, DataFile> dataFilesByPath,
      Map<String, DeleteFile> existingDvsByDataFile,
      Map<String, Long> eqSequenceNums,
      Map<String, Long> dataSequenceNums) {

    Map<String, List<DeleteFile>> eqByPartition = groupByPartitionKey(eqDeleteFiles);
    Map<String, Map<String, DataFile>> dataByPartition = new HashMap<>();
    for (Map.Entry<String, DataFile> entry : dataFilesByPath.entrySet()) {
      String key = partitionKey(entry.getValue());
      dataByPartition
          .computeIfAbsent(key, k -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
    }

    Map<String, PartitionWorkUnit> workUnits = new HashMap<>();
    for (Map.Entry<String, List<DeleteFile>> entry : eqByPartition.entrySet()) {
      String key = entry.getKey();
      Map<String, DataFile> partitionDataFiles =
          dataByPartition.getOrDefault(key, Collections.emptyMap());

      Map<String, DeleteFile> partitionDvs = new HashMap<>();
      for (String dataPath : partitionDataFiles.keySet()) {
        DeleteFile dv = existingDvsByDataFile.get(dataPath);
        if (dv != null) {
          partitionDvs.put(dataPath, dv);
        }
      }

      workUnits.put(
          key,
          new PartitionWorkUnit(
              entry.getValue(),
              partitionDataFiles,
              partitionDvs,
              eqSequenceNums,
              dataSequenceNums));
    }

    return workUnits;
  }

  private <F extends ContentFile<F>> Map<String, List<F>> groupByPartitionKey(List<F> files) {
    Map<String, List<F>> groups = new HashMap<>();
    for (F file : files) {
      groups.computeIfAbsent(partitionKey(file), k -> new ArrayList<>()).add(file);
    }
    return groups;
  }

  private String partitionKey(ContentFile<?> file) {
    PartitionSpec spec = table.specs().get(file.specId());
    if (!spec.isPartitioned()) {
      return "unpartitioned";
    }

    StructLike partition = file.partition();
    StringBuilder sb = new StringBuilder().append(file.specId());
    for (int i = 0; i < partition.size(); i++) {
      sb.append("|").append(partition.get(i, Object.class));
    }
    return sb.toString();
  }

  // --- Commit & DV building ---

  private void commitConversion(
      long startingSnapshotId,
      List<DeleteFile> eqDeleteFiles,
      Map<String, DeleteFile> existingDvsByDataFile,
      List<DeleteFile> newDvs) {

    Set<String> dvReferencedFiles =
        newDvs.stream().map(dv -> dv.referencedDataFile().toString()).collect(Collectors.toSet());

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshotId);
    eqDeleteFiles.forEach(rewrite::deleteFile);
    existingDvsByDataFile.entrySet().stream()
        .filter(e -> dvReferencedFiles.contains(e.getKey()))
        .map(Map.Entry::getValue)
        .forEach(rewrite::deleteFile);
    newDvs.forEach(rewrite::addFile);
    commit(rewrite);
  }

  private List<DeleteFile> buildDvDeleteFiles(
      List<Row> dvResultRows, Map<String, DataFile> dataFilesByPath) {
    List<DeleteFile> newDvs = new ArrayList<>();
    for (Row row : dvResultRows) {
      String referencedDataFile = row.getString(0);
      DataFile dataFile = dataFilesByPath.get(referencedDataFile);
      Preconditions.checkState(
          dataFile != null, "Data file not found in inventory: %s", referencedDataFile);
      PartitionSpec spec = table.specs().get(dataFile.specId());

      newDvs.add(
          FileMetadata.deleteFileBuilder(spec)
              .ofPositionDeletes()
              .withFormat(FileFormat.PUFFIN)
              .withPath(row.getString(1))
              .withPartition(dataFile.partition())
              .withFileSizeInBytes(row.getLong(2))
              .withReferencedDataFile(referencedDataFile)
              .withContentOffset(row.getLong(3))
              .withContentSizeInBytes(row.getLong(4))
              .withRecordCount(row.getLong(5))
              .build());
    }
    return newDvs;
  }

  // --- Utility ---

  private Dataset<Row> applyJoinStrategy(Dataset<Row> eqDeletes) {
    String strategy = PropertyUtil.propertyAsString(options(), JOIN_STRATEGY, JOIN_STRATEGY_AUTO);
    switch (strategy) {
      case JOIN_STRATEGY_BROADCAST:
        return functions.broadcast(eqDeletes);
      case JOIN_STRATEGY_HASH:
        return eqDeletes.hint("SHUFFLE_HASH");
      case JOIN_STRATEGY_SORT_MERGE:
        return eqDeletes.hint("MERGE");
      default:
        return eqDeletes;
    }
  }

  private static ExecutorService partitionExecutorService(int concurrency) {
    return MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor)
            Executors.newFixedThreadPool(
                concurrency,
                new ThreadFactoryBuilder().setNameFormat("convert-eq-deletes-%d").build()));
  }

  private Dataset<Row> createFileListDF(List<DeleteFile> files, Map<String, Long> sequenceNums) {
    List<Row> rows =
        files.stream()
            .map(
                f ->
                    RowFactory.create(
                        f.location(),
                        f.format().name(),
                        sequenceNums.getOrDefault(f.location(), 0L)))
            .collect(Collectors.toList());
    return spark().createDataFrame(rows, FILE_LIST_SCHEMA);
  }

  private Map<String, DvInfo> buildDvInfoMap(Map<String, DeleteFile> existingDvsByDataFile) {
    Map<String, DvInfo> map = new HashMap<>();
    existingDvsByDataFile.forEach(
        (path, dv) ->
            map.put(
                path,
                new DvInfo(
                    dv.location(),
                    dv.contentOffset(),
                    dv.contentSizeInBytes(),
                    dv.recordCount(),
                    path)));
    return map;
  }

  private static List<Types.NestedField> appendField(
      List<Types.NestedField> fields, Types.NestedField extra) {
    List<Types.NestedField> result = new ArrayList<>(fields);
    result.add(extra);
    return result;
  }

  private List<DeleteFile> collectDeleteFiles(
      Dataset<Row> dataFileStarDF, Types.StructType combinedFileType) {
    StructType sparkType = dataFileStarDF.schema();
    return dataFileStarDF.collectAsList().stream()
        .map(row -> wrapDeleteFile(combinedFileType, sparkType, row))
        .collect(Collectors.toList());
  }

  private Map<String, DataFile> collectDataFiles(
      Dataset<Row> filteredEntries, Types.StructType combinedFileType) {
    Dataset<Row> df = filteredEntries.select("data_file.*");
    StructType sparkType = df.schema();
    Map<String, DataFile> map = new HashMap<>();
    df.collectAsList()
        .forEach(
            row -> {
              DataFile file = wrapDataFile(combinedFileType, sparkType, row);
              map.put(file.location(), file);
            });
    return map;
  }

  private Map<String, DeleteFile> collectExistingDvs(
      Dataset<Row> dvEntries, Types.StructType combinedFileType) {
    Dataset<Row> df = dvEntries.select("data_file.*");
    StructType sparkType = df.schema();
    Map<String, DeleteFile> map = new HashMap<>();
    df.collectAsList()
        .forEach(
            row -> {
              DeleteFile dv = wrapDeleteFile(combinedFileType, sparkType, row);
              if (dv.referencedDataFile() != null) {
                map.put(dv.referencedDataFile().toString(), dv);
              }
            });
    return map;
  }

  private DeleteFile wrapDeleteFile(
      Types.StructType combinedFileType, StructType sparkType, Row row) {
    int specId = row.getInt(row.fieldIndex("spec_id"));
    Types.StructType projection = DataFile.getType(table.specs().get(specId).partitionType());
    return new SparkDeleteFile(combinedFileType, projection, sparkType).wrap(row);
  }

  private DataFile wrapDataFile(Types.StructType combinedFileType, StructType sparkType, Row row) {
    int specId = row.getInt(row.fieldIndex("spec_id"));
    Types.StructType projection = DataFile.getType(table.specs().get(specId).partitionType());
    return new SparkDataFile(combinedFileType, projection, sparkType).wrap(row);
  }

  // --- Inner classes ---

  static class PartitionWorkUnit {
    final List<DeleteFile> eqDeleteFiles;
    final Map<String, DataFile> dataFiles;
    final Map<String, DeleteFile> existingDvs;
    final Map<String, Long> eqSequenceNums;
    final Map<String, Long> dataSequenceNums;

    PartitionWorkUnit(
        List<DeleteFile> eqDeleteFiles,
        Map<String, DataFile> dataFiles,
        Map<String, DeleteFile> existingDvs,
        Map<String, Long> eqSequenceNums,
        Map<String, Long> dataSequenceNums) {
      this.eqDeleteFiles = eqDeleteFiles;
      this.dataFiles = dataFiles;
      this.existingDvs = existingDvs;
      this.eqSequenceNums = eqSequenceNums;
      this.dataSequenceNums = dataSequenceNums;
    }
  }

  static class DvInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    final String location;
    final long contentOffset;
    final long contentSize;
    final long recordCount;
    final String referencedDataFile;

    DvInfo(
        String location,
        long contentOffset,
        long contentSize,
        long recordCount,
        String referencedDataFile) {
      this.location = location;
      this.contentOffset = contentOffset;
      this.contentSize = contentSize;
      this.recordCount = recordCount;
      this.referencedDataFile = referencedDataFile;
    }
  }

  /**
   * Reads Iceberg files using {@link FormatModelRegistry}. Vectorized for Parquet/ORC, row-by-row
   * for Avro. Returns a lazy iterator.
   */
  static class IcebergFileReader implements FlatMapFunction<Row, Row> {
    private final Broadcast<Table> tableBroadcast;
    private final Schema icebergSchema;
    private final StructType equalitySparkSchema;
    private final boolean includeRowPosition;

    IcebergFileReader(
        Broadcast<Table> tableBroadcast,
        Schema icebergSchema,
        StructType equalitySparkSchema,
        boolean includeRowPosition) {
      this.tableBroadcast = tableBroadcast;
      this.icebergSchema = icebergSchema;
      this.equalitySparkSchema = equalitySparkSchema;
      this.includeRowPosition = includeRowPosition;
    }

    @Override
    public Iterator<Row> call(Row fileMetadata) throws Exception {
      String filePath = fileMetadata.getString(0);
      FileFormat format = FileFormat.fromString(fileMetadata.getString(1));
      long sequenceNum = fileMetadata.getLong(2);

      Table tbl = tableBroadcast.value();
      InputFile inputFile = tbl.io().newInputFile(filePath);

      int eqFieldCount =
          includeRowPosition ? icebergSchema.columns().size() - 1 : icebergSchema.columns().size();

      StructField[] sparkFields = equalitySparkSchema.fields();
      DataType[] sparkTypes = new DataType[eqFieldCount];
      for (int i = 0; i < eqFieldCount; i++) {
        sparkTypes[i] = sparkFields[i].dataType();
      }

      if (canVectorize(format, icebergSchema)) {
        CloseableIterable<ColumnarBatch> batches =
            FormatModelRegistry.<ColumnarBatch, StructType>readBuilder(
                    format, ColumnarBatch.class, inputFile)
                .project(icebergSchema)
                .recordsPerBatch(DEFAULT_BATCH_SIZE)
                .build();
        return new BatchRowIterator(
            batches, sparkTypes, eqFieldCount, filePath, sequenceNum, includeRowPosition);
      } else {
        CloseableIterable<InternalRow> rows =
            FormatModelRegistry.<InternalRow, StructType>readBuilder(
                    format, InternalRow.class, inputFile)
                .project(icebergSchema)
                .build();
        return new InternalRowIterator(
            rows, sparkTypes, eqFieldCount, filePath, sequenceNum, includeRowPosition);
      }
    }

    private static boolean canVectorize(FileFormat format, Schema schema) {
      return VECTORIZABLE_FORMATS.contains(format)
          && schema.columns().stream()
              .allMatch(
                  f -> f.type().isPrimitiveType() || MetadataColumns.isMetadataColumn(f.fieldId()));
    }

    static Row convertRow(
        InternalRow internalRow,
        DataType[] sparkTypes,
        int eqFieldCount,
        String filePath,
        long sequenceNum,
        boolean includeRowPosition) {
      int extraCols = includeRowPosition ? 3 : 1;
      Object[] values = new Object[eqFieldCount + extraCols];

      for (int i = 0; i < eqFieldCount; i++) {
        if (!internalRow.isNullAt(i)) {
          values[i] =
              CatalystTypeConverters.convertToScala(
                  internalRow.get(i, sparkTypes[i]), sparkTypes[i]);
        }
      }

      if (includeRowPosition) {
        values[eqFieldCount] = internalRow.getLong(eqFieldCount);
        values[eqFieldCount + 1] = filePath;
        values[eqFieldCount + 2] = sequenceNum;
      } else {
        values[eqFieldCount] = sequenceNum;
      }

      return RowFactory.create(values);
    }
  }

  private static class BatchRowIterator implements Iterator<Row> {
    private final CloseableIterable<ColumnarBatch> batches;
    private final Iterator<ColumnarBatch> batchIter;
    private final DataType[] sparkTypes;
    private final int eqFieldCount;
    private final String filePath;
    private final long sequenceNum;
    private final boolean includeRowPosition;
    private ColumnarBatch currentBatch;
    private int currentRowIdx;

    BatchRowIterator(
        CloseableIterable<ColumnarBatch> batches,
        DataType[] sparkTypes,
        int eqFieldCount,
        String filePath,
        long sequenceNum,
        boolean includeRowPosition) {
      this.batches = batches;
      this.batchIter = batches.iterator();
      this.sparkTypes = sparkTypes;
      this.eqFieldCount = eqFieldCount;
      this.filePath = filePath;
      this.sequenceNum = sequenceNum;
      this.includeRowPosition = includeRowPosition;
      advanceBatch();
    }

    @Override
    public boolean hasNext() {
      while (currentBatch != null && currentRowIdx >= currentBatch.numRows()) {
        advanceBatch();
      }

      if (currentBatch == null) {
        try {
          batches.close();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return false;
      }
      return true;
    }

    @Override
    public Row next() {
      Row row =
          IcebergFileReader.convertRow(
              currentBatch.getRow(currentRowIdx),
              sparkTypes,
              eqFieldCount,
              filePath,
              sequenceNum,
              includeRowPosition);
      currentRowIdx++;
      return row;
    }

    private void advanceBatch() {
      currentBatch = batchIter.hasNext() ? batchIter.next() : null;
      currentRowIdx = 0;
    }
  }

  private static class InternalRowIterator implements Iterator<Row> {
    private final CloseableIterable<InternalRow> iterable;
    private final Iterator<InternalRow> iter;
    private final DataType[] sparkTypes;
    private final int eqFieldCount;
    private final String filePath;
    private final long sequenceNum;
    private final boolean includeRowPosition;

    InternalRowIterator(
        CloseableIterable<InternalRow> iterable,
        DataType[] sparkTypes,
        int eqFieldCount,
        String filePath,
        long sequenceNum,
        boolean includeRowPosition) {
      this.iterable = iterable;
      this.iter = iterable.iterator();
      this.sparkTypes = sparkTypes;
      this.eqFieldCount = eqFieldCount;
      this.filePath = filePath;
      this.sequenceNum = sequenceNum;
      this.includeRowPosition = includeRowPosition;
    }

    @Override
    public boolean hasNext() {
      boolean has = iter.hasNext();
      if (!has) {
        try {
          iterable.close();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      return has;
    }

    @Override
    public Row next() {
      return IcebergFileReader.convertRow(
          iter.next(), sparkTypes, eqFieldCount, filePath, sequenceNum, includeRowPosition);
    }
  }

  /** Writes deletion vectors on executors using {@link BaseDVFileWriter}. */
  static class WriteDVs implements MapPartitionsFunction<Row, Row> {
    private final Broadcast<Table> tableBroadcast;
    private final Broadcast<Map<String, DvInfo>> dvMapBroadcast;

    WriteDVs(Broadcast<Table> tableBroadcast, Broadcast<Map<String, DvInfo>> dvMapBroadcast) {
      this.tableBroadcast = tableBroadcast;
      this.dvMapBroadcast = dvMapBroadcast;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> input) throws Exception {
      if (!input.hasNext()) {
        return Collections.emptyIterator();
      }

      Table tbl = tableBroadcast.value();
      Map<String, DvInfo> dvMap = dvMapBroadcast.value();
      TaskContext ctx = TaskContext.get();
      int partitionId = ctx != null ? ctx.partitionId() : 0;
      long taskId = ctx != null ? ctx.taskAttemptId() : 0;

      OutputFileFactory fileFactory =
          OutputFileFactory.builderFor(tbl, partitionId, taskId).format(FileFormat.PUFFIN).build();

      BaseDVFileWriter dvWriter =
          new BaseDVFileWriter(fileFactory, path -> loadExistingDv(tbl, dvMap.get(path)));

      PartitionSpec dummySpec = PartitionSpec.unpartitioned();
      while (input.hasNext()) {
        Row row = input.next();
        dvWriter.delete(row.getString(0), row.getLong(1), dummySpec, null);
      }

      dvWriter.close();
      DeleteWriteResult writeResult = dvWriter.result();

      List<Row> results = new ArrayList<>();
      for (DeleteFile dv : writeResult.deleteFiles()) {
        results.add(
            RowFactory.create(
                dv.referencedDataFile().toString(),
                dv.location(),
                dv.fileSizeInBytes(),
                dv.contentOffset(),
                dv.contentSizeInBytes(),
                dv.recordCount()));
      }
      return results.iterator();
    }

    private static PositionDeleteIndex loadExistingDv(Table tbl, DvInfo dvInfo) {
      if (dvInfo == null) {
        return null;
      }
      try {
        InputFile inputFile = tbl.io().newInputFile(dvInfo.location);
        byte[] bytes = new byte[Math.toIntExact(dvInfo.contentSize)];
        IOUtil.readFully(inputFile, dvInfo.contentOffset, bytes, 0, bytes.length);
        DeleteFile dvMetadata =
            FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withFormat(FileFormat.PUFFIN)
                .withPath(dvInfo.location)
                .withFileSizeInBytes(0)
                .withRecordCount(dvInfo.recordCount)
                .withContentOffset(dvInfo.contentOffset)
                .withContentSizeInBytes(dvInfo.contentSize)
                .withReferencedDataFile(dvInfo.referencedDataFile)
                .build();
        return PositionDeleteIndex.deserialize(bytes, dvMetadata);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read existing DV at " + dvInfo.location, e);
      }
    }
  }
}
