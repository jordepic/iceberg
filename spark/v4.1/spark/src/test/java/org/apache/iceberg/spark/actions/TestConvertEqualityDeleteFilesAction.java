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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.actions.ConvertEqualityDeleteFiles;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestConvertEqualityDeleteFilesAction extends TestBase {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()),
          Types.NestedField.required(3, "category", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("category").build();

  @TempDir private Path temp;
  private String tableLocation;
  private Table table;

  @BeforeEach
  public void setupTable() {
    tableLocation = temp.resolve("table").toString();
    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    table = tables.create(SCHEMA, SPEC, ImmutableMap.of("format-version", "3"), tableLocation);
  }

  @Test
  public void testSinglePartitionDelete() throws IOException {
    writeDataFiles();

    Schema deleteSchema = table.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteSchema);

    DeleteFile eqDelete =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecord.copy("data", "a")),
            deleteSchema);
    table.newRowDelta().addDeletes(eqDelete).commit();

    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark).convertEqualityDeletes(table).execute();
    table.refresh();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(1);
    assertThat(result.addedDeletionVectorCount()).isGreaterThan(0);
    assertThat(result.addedPositionDeleteFilesCount()).isEqualTo(0);

    assertThat(eqDeleteFileCount()).isEqualTo(0);
    assertThat(dvFileCount()).isGreaterThan(0);

    // DVs only in cat1 partition (where the eq-delete was)
    assertThat(dvCountInPartition("cat1")).isGreaterThan(0);
    assertThat(dvCountInPartition("cat2")).isEqualTo(0);

    Dataset<Row> liveData = spark.read().format("iceberg").load(tableLocation);
    assertThat(liveData.count()).isEqualTo(4);
    assertThat(liveData.filter("data = 'a' AND category = 'cat1'").count()).isEqualTo(0);
    assertThat(liveData.filter("data = 'b' AND category = 'cat1'").count()).isEqualTo(1);
    assertThat(liveData.filter("category = 'cat2'").count()).isEqualTo(2);
  }

  @Test
  public void testMultiPartitionDeletes() throws IOException {
    writeDataFiles();

    Schema deleteSchema = table.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteSchema);

    DeleteFile eqDelete1 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecord.copy("data", "a")),
            deleteSchema);

    DeleteFile eqDelete2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat2"),
            Lists.newArrayList(deleteRecord.copy("data", "d")),
            deleteSchema);

    table.newRowDelta().addDeletes(eqDelete1).addDeletes(eqDelete2).commit();

    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark).convertEqualityDeletes(table).execute();
    table.refresh();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(2);
    assertThat(eqDeleteFileCount()).isEqualTo(0);

    // DVs in both partitions
    assertThat(dvCountInPartition("cat1")).isGreaterThan(0);
    assertThat(dvCountInPartition("cat2")).isGreaterThan(0);

    Dataset<Row> liveData = spark.read().format("iceberg").load(tableLocation);
    assertThat(liveData.count()).isEqualTo(3);
    assertThat(liveData.filter("data = 'a' AND category = 'cat1'").count()).isEqualTo(0);
    assertThat(liveData.filter("data = 'b' AND category = 'cat1'").count()).isEqualTo(1);
    assertThat(liveData.filter("data = 'c' AND category = 'cat1'").count()).isEqualTo(1);
    assertThat(liveData.filter("data = 'd' AND category = 'cat2'").count()).isEqualTo(0);
    assertThat(liveData.filter("data = 'e' AND category = 'cat2'").count()).isEqualTo(1);
  }

  @Test
  public void testNoEqualityDeletes() {
    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark).convertEqualityDeletes(table).execute();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(0);
    assertThat(result.addedDeletionVectorCount()).isEqualTo(0);
  }

  @Test
  public void testIdempotent() throws IOException {
    writeDataFiles();

    Schema deleteSchema = table.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteSchema);
    DeleteFile eqDelete =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecord.copy("data", "a")),
            deleteSchema);
    table.newRowDelta().addDeletes(eqDelete).commit();

    SparkActions.get(spark).convertEqualityDeletes(table).execute();
    table.refresh();

    ConvertEqualityDeleteFiles.Result secondRun =
        SparkActions.get(spark).convertEqualityDeletes(table).execute();

    assertThat(secondRun.convertedEqualityDeleteFilesCount()).isEqualTo(0);
    assertThat(secondRun.addedDeletionVectorCount()).isEqualTo(0);
  }

  @Test
  public void testMultipleDataFilesPerPartition() throws IOException {
    Record record = GenericRecord.create(SCHEMA);

    DataFile dataFile1 =
        FileHelpers.writeDataFile(
            table,
            Files.localOutput(File.createTempFile("data", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(
                record.copy("id", 1, "data", "a", "category", "cat1"),
                record.copy("id", 2, "data", "b", "category", "cat1")));

    DataFile dataFile2 =
        FileHelpers.writeDataFile(
            table,
            Files.localOutput(File.createTempFile("data", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(
                record.copy("id", 3, "data", "a", "category", "cat1"),
                record.copy("id", 4, "data", "c", "category", "cat1")));

    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    Schema deleteSchema = table.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteSchema);
    DeleteFile eqDelete =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecord.copy("data", "a")),
            deleteSchema);
    table.newRowDelta().addDeletes(eqDelete).commit();

    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark).convertEqualityDeletes(table).execute();
    table.refresh();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(1);
    // DVs should reference both data files (both contain data='a')
    assertThat(result.addedDeletionVectorCount()).isGreaterThanOrEqualTo(1);

    Dataset<Row> liveData = spark.read().format("iceberg").load(tableLocation);
    assertThat(liveData.count()).isEqualTo(2);
    assertThat(liveData.filter("data = 'a'").count()).isEqualTo(0);
    assertThat(liveData.filter("data = 'b'").count()).isEqualTo(1);
    assertThat(liveData.filter("data = 'c'").count()).isEqualTo(1);
  }

  private void writeDataFiles() throws IOException {
    Record record = GenericRecord.create(SCHEMA);

    DataFile dataFile1 =
        FileHelpers.writeDataFile(
            table,
            Files.localOutput(File.createTempFile("data", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(
                record.copy("id", 1, "data", "a", "category", "cat1"),
                record.copy("id", 2, "data", "b", "category", "cat1"),
                record.copy("id", 3, "data", "c", "category", "cat1")));

    DataFile dataFile2 =
        FileHelpers.writeDataFile(
            table,
            Files.localOutput(File.createTempFile("data", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat2"),
            Lists.newArrayList(
                record.copy("id", 4, "data", "d", "category", "cat2"),
                record.copy("id", 5, "data", "e", "category", "cat2")));

    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();
  }

  private long eqDeleteFileCount() {
    return spark
        .read()
        .format("iceberg")
        .load(tableLocation + "#delete_files")
        .filter("content = 2")
        .count();
  }

  private long dvFileCount() {
    return spark
        .read()
        .format("iceberg")
        .load(tableLocation + "#delete_files")
        .filter("UPPER(file_format) = 'PUFFIN'")
        .count();
  }

  @Test
  public void testFilterExpression() throws IOException {
    writeDataFiles();

    Schema deleteSchema = table.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteSchema);

    DeleteFile eqDelete1 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecord.copy("data", "a")),
            deleteSchema);
    DeleteFile eqDelete2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat2"),
            Lists.newArrayList(deleteRecord.copy("data", "d")),
            deleteSchema);
    table.newRowDelta().addDeletes(eqDelete1).addDeletes(eqDelete2).commit();

    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark)
            .convertEqualityDeletes(table)
            .filter(Expressions.equal("category", "cat1"))
            .execute();
    table.refresh();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(1);
    // cat2 eq-delete should still remain unconverted
    assertThat(eqDeleteFileCount()).isEqualTo(1);
    assertThat(dvCountInPartition("cat1")).isGreaterThan(0);
    assertThat(dvCountInPartition("cat2")).isEqualTo(0);
  }

  @Test
  public void testMaxPartitions() throws IOException {
    writeDataFiles();

    Schema deleteSchema = table.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteSchema);

    DeleteFile eqDelete1 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecord.copy("data", "a")),
            deleteSchema);
    DeleteFile eqDelete2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat2"),
            Lists.newArrayList(deleteRecord.copy("data", "d")),
            deleteSchema);
    table.newRowDelta().addDeletes(eqDelete1).addDeletes(eqDelete2).commit();

    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark)
            .convertEqualityDeletes(table)
            .option(ConvertEqualityDeleteFilesSparkAction.MAX_PARTITIONS, "1")
            .execute();
    table.refresh();

    // Only 1 partition processed
    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(1);
    // One eq-delete remains in the other partition
    assertThat(eqDeleteFileCount()).isEqualTo(1);
  }

  @Test
  public void testUnpartitionedTable() throws IOException {
    String unpartitionedLocation = temp.resolve("unpartitioned").toString();
    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table unpartitioned =
        tables.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", "3"),
            unpartitionedLocation);

    Record record = GenericRecord.create(SCHEMA);
    DataFile dataFile =
        FileHelpers.writeDataFile(
            unpartitioned,
            Files.localOutput(File.createTempFile("data", ".parquet", temp.toFile())),
            Lists.newArrayList(
                record.copy("id", 1, "data", "a", "category", "cat1"),
                record.copy("id", 2, "data", "b", "category", "cat1")));
    unpartitioned.newAppend().appendFile(dataFile).commit();

    Schema deleteSchema = unpartitioned.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteSchema);
    DeleteFile eqDelete =
        FileHelpers.writeDeleteFile(
            unpartitioned,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            Lists.newArrayList(deleteRecord.copy("data", "a")),
            deleteSchema);
    unpartitioned.newRowDelta().addDeletes(eqDelete).commit();

    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark).convertEqualityDeletes(unpartitioned).execute();
    unpartitioned.refresh();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(1);
    assertThat(result.addedDeletionVectorCount()).isGreaterThan(0);

    Dataset<Row> liveData = spark.read().format("iceberg").load(unpartitionedLocation);
    assertThat(liveData.count()).isEqualTo(1);
    assertThat(liveData.filter("data = 'a'").count()).isEqualTo(0);
    assertThat(liveData.filter("data = 'b'").count()).isEqualTo(1);
  }

  @Test
  public void testOrcDataFiles() throws IOException {
    String orcLocation = temp.resolve("orc_table").toString();
    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table orcTable =
        tables.create(
            SCHEMA,
            SPEC,
            ImmutableMap.of("format-version", "3", "write.format.default", "orc"),
            orcLocation);

    Record record = GenericRecord.create(SCHEMA);
    DataFile dataFile =
        FileHelpers.writeDataFile(
            orcTable,
            Files.localOutput(File.createTempFile("data", ".orc", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(
                record.copy("id", 1, "data", "a", "category", "cat1"),
                record.copy("id", 2, "data", "b", "category", "cat1")));
    orcTable.newAppend().appendFile(dataFile).commit();

    // Eq-delete files are always Parquet (written by Flink/writers)
    Schema deleteSchema = orcTable.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteSchema);
    DeleteFile eqDelete =
        FileHelpers.writeDeleteFile(
            orcTable,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecord.copy("data", "a")),
            deleteSchema);
    orcTable.newRowDelta().addDeletes(eqDelete).commit();

    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark).convertEqualityDeletes(orcTable).execute();
    orcTable.refresh();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(1);
    assertThat(result.addedDeletionVectorCount()).isGreaterThan(0);

    Dataset<Row> liveData = spark.read().format("iceberg").load(orcLocation);
    assertThat(liveData.count()).isEqualTo(1);
    assertThat(liveData.filter("data = 'a'").count()).isEqualTo(0);
    assertThat(liveData.filter("data = 'b'").count()).isEqualTo(1);
  }

  @Test
  public void testExistingDvMerge() throws IOException {
    writeDataFiles();

    Schema deleteSchema = table.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteSchema);

    // First pass: convert eq-delete for data='a'
    DeleteFile eqDelete1 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecord.copy("data", "a")),
            deleteSchema);
    table.newRowDelta().addDeletes(eqDelete1).commit();

    SparkActions.get(spark).convertEqualityDeletes(table).execute();
    table.refresh();

    long dvsAfterFirstPass = dvFileCount();
    assertThat(dvsAfterFirstPass).isGreaterThan(0);

    // Second pass: add eq-delete for data='b' (same partition, same data file)
    DeleteFile eqDelete2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecord.copy("data", "b")),
            deleteSchema);
    table.newRowDelta().addDeletes(eqDelete2).commit();

    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark).convertEqualityDeletes(table).execute();
    table.refresh();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(1);
    assertThat(eqDeleteFileCount()).isEqualTo(0);

    // Both data='a' and data='b' should be deleted, only data='c' survives in cat1
    Dataset<Row> liveData = spark.read().format("iceberg").load(tableLocation);
    assertThat(liveData.filter("category = 'cat1'").count()).isEqualTo(1);
    assertThat(liveData.filter("data = 'a'").count()).isEqualTo(0);
    assertThat(liveData.filter("data = 'b'").count()).isEqualTo(0);
    assertThat(liveData.filter("data = 'c'").count()).isEqualTo(1);
    assertThat(liveData.filter("category = 'cat2'").count()).isEqualTo(2);
  }

  @Test
  public void testAvroDataFiles() throws IOException {
    String avroLocation = temp.resolve("avro_table").toString();
    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table avroTable =
        tables.create(
            SCHEMA,
            SPEC,
            ImmutableMap.of("format-version", "3", "write.format.default", "avro"),
            avroLocation);

    Record record = GenericRecord.create(SCHEMA);
    DataFile dataFile =
        FileHelpers.writeDataFile(
            avroTable,
            Files.localOutput(File.createTempFile("data", ".avro", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(
                record.copy("id", 1, "data", "a", "category", "cat1"),
                record.copy("id", 2, "data", "b", "category", "cat1")));
    avroTable.newAppend().appendFile(dataFile).commit();

    Schema deleteSchema = avroTable.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteSchema);
    DeleteFile eqDelete =
        FileHelpers.writeDeleteFile(
            avroTable,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecord.copy("data", "a")),
            deleteSchema);
    avroTable.newRowDelta().addDeletes(eqDelete).commit();

    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark).convertEqualityDeletes(avroTable).execute();
    avroTable.refresh();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(1);
    assertThat(result.addedDeletionVectorCount()).isGreaterThan(0);

    Dataset<Row> liveData = spark.read().format("iceberg").load(avroLocation);
    assertThat(liveData.count()).isEqualTo(1);
    assertThat(liveData.filter("data = 'a'").count()).isEqualTo(0);
    assertThat(liveData.filter("data = 'b'").count()).isEqualTo(1);
  }

  @Test
  public void testMixedDataFileFormats() throws IOException {
    Record record = GenericRecord.create(SCHEMA);

    // Write one Parquet data file
    DataFile parquetFile =
        FileHelpers.writeDataFile(
            table,
            Files.localOutput(File.createTempFile("data", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(record.copy("id", 1, "data", "a", "category", "cat1")));

    // Write one ORC data file in the same partition
    String orcLocation = temp.resolve("orc_mixed").toString();
    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table orcHelper =
        tables.create(
            SCHEMA,
            SPEC,
            ImmutableMap.of("format-version", "3", "write.format.default", "orc"),
            orcLocation);
    DataFile orcFile =
        FileHelpers.writeDataFile(
            orcHelper,
            Files.localOutput(File.createTempFile("data", ".orc", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(record.copy("id", 2, "data", "a", "category", "cat1")));

    table.newAppend().appendFile(parquetFile).appendFile(orcFile).commit();

    Schema deleteSchema = table.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteSchema);
    DeleteFile eqDelete =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecord.copy("data", "a")),
            deleteSchema);
    table.newRowDelta().addDeletes(eqDelete).commit();

    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark).convertEqualityDeletes(table).execute();
    table.refresh();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(1);
    assertThat(result.addedDeletionVectorCount()).isGreaterThanOrEqualTo(2);
    assertThat(eqDeleteFileCount()).isEqualTo(0);

    Dataset<Row> liveData = spark.read().format("iceberg").load(tableLocation);
    assertThat(liveData.filter("data = 'a'").count()).isEqualTo(0);
  }

  @Test
  public void testSchemaEvolution() throws IOException {
    Record record = GenericRecord.create(SCHEMA);

    DataFile dataFile1 =
        FileHelpers.writeDataFile(
            table,
            Files.localOutput(File.createTempFile("data", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(
                record.copy("id", 1, "data", "a", "category", "cat1"),
                record.copy("id", 2, "data", "b", "category", "cat1")));
    table.newAppend().appendFile(dataFile1).commit();

    // Evolve schema: add a new column
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();

    // Write more data with the new schema
    Schema evolvedSchema = table.schema();
    Record evolvedRecord = GenericRecord.create(evolvedSchema);
    DataFile dataFile2 =
        FileHelpers.writeDataFile(
            table,
            Files.localOutput(File.createTempFile("data", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(
                evolvedRecord.copy(
                    ImmutableMap.of("id", 3, "data", "a", "category", "cat1", "extra", "x")),
                evolvedRecord.copy(
                    ImmutableMap.of("id", 4, "data", "c", "category", "cat1", "extra", "y"))));
    table.newAppend().appendFile(dataFile2).commit();

    // Eq-delete on 'data' column (exists in both old and new schema)
    Schema deleteSchema = table.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteSchema);
    DeleteFile eqDelete =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecord.copy("data", "a")),
            deleteSchema);
    table.newRowDelta().addDeletes(eqDelete).commit();

    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark).convertEqualityDeletes(table).execute();
    table.refresh();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(1);
    assertThat(eqDeleteFileCount()).isEqualTo(0);

    // data='a' deleted from both old-schema and new-schema files
    Dataset<Row> liveData = spark.read().format("iceberg").load(tableLocation);
    assertThat(liveData.filter("data = 'a'").count()).isEqualTo(0);
    assertThat(liveData.filter("data = 'b'").count()).isEqualTo(1);
    assertThat(liveData.filter("data = 'c'").count()).isEqualTo(1);
    assertThat(liveData.count()).isEqualTo(2);
  }

  @Test
  public void testMultipleEqualitySchemas() throws IOException {
    writeDataFiles();

    // Group A: eq-delete on {data} — deletes data='a' in cat1
    Schema deleteSchemaA = table.schema().select("data");
    Record deleteRecordA = GenericRecord.create(deleteSchemaA);
    DeleteFile eqDeleteA =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecordA.copy("data", "c")),
            deleteSchemaA);

    // Group B: eq-delete on {id} — deletes id=1 (which is data='a') in cat1
    Schema deleteSchemaB = table.schema().select("id");
    Record deleteRecordB = GenericRecord.create(deleteSchemaB);
    DeleteFile eqDeleteB =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("eq-delete", ".parquet", temp.toFile())),
            TestHelpers.Row.of("cat1"),
            Lists.newArrayList(deleteRecordB.copy("id", 1)),
            deleteSchemaB);

    table.newRowDelta().addDeletes(eqDeleteA).addDeletes(eqDeleteB).commit();

    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get(spark).convertEqualityDeletes(table).execute();
    table.refresh();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(2);
    assertThat(eqDeleteFileCount()).isEqualTo(0);

    // data='a' (id=1) deleted by group B, data='c' (id=3) deleted by group A
    Dataset<Row> liveData = spark.read().format("iceberg").load(tableLocation);
    assertThat(liveData.filter("category = 'cat1'").count()).isEqualTo(1);
    assertThat(liveData.filter("data = 'a'").count()).isEqualTo(0);
    assertThat(liveData.filter("data = 'b'").count()).isEqualTo(1);
    assertThat(liveData.filter("data = 'c'").count()).isEqualTo(0);
    assertThat(liveData.filter("category = 'cat2'").count()).isEqualTo(2);
  }

  private long dvCountInPartition(String category) {
    return spark
        .read()
        .format("iceberg")
        .load(tableLocation + "#delete_files")
        .filter("UPPER(file_format) = 'PUFFIN'")
        .filter("partition.category = '" + category + "'")
        .count();
  }
}
