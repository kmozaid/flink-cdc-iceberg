package com.example.flink.iceberg;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;


public class CdcIcebergJob {

  private static final String WAREHOUSE_LOCATION = "hdfs://localhost:9000/user/iceberg/warehouse";
  private static final String METASTORE_URI = "thrift://localhost:9083";

  public static void main(String[] args)
      throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // This is must be 1.
    env.setParallelism(1);

    DataStream<JobStatusCdc> jobStatusCdcStream = getJobStatusCdcStream(env);

    DataStream<RowData> jobStatusRowDataStream = jobStatusCdcStream.map(jobStatusCdc -> {
      GenericRowData cdcRow = new GenericRowData(rowKind(jobStatusCdc.op), 2);
      cdcRow.setField(0, jobStatusCdc.id);
      cdcRow.setField(1, StringData.fromString(jobStatusCdc.status));
      return cdcRow;
    });

    FlinkSink.forRowData(jobStatusRowDataStream)
        .tableLoader(TableLoader.fromCatalog(getCatalogLoader(), TableIdentifier.of("datawizards", "job_status")))
        .writeParallelism(1)
        .equalityFieldColumns(Stream.of("id").collect(Collectors.toList()))
        .upsert(true)
        .append();

    // execute program
    env.execute("Sink CDC in Iceberg");
  }

  private static CatalogLoader getCatalogLoader() {
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, METASTORE_URI);
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE_LOCATION);
    return CatalogLoader.hive("hive_local", new Configuration(), properties);
  }

  private static DataStreamSource<JobStatusCdc> getJobStatusCdcStream(StreamExecutionEnvironment env) {
    return env.fromElements(JobStatusCdc.class,
        new JobStatusCdc(Op.I, 0, "start"),
        new JobStatusCdc(Op.I, 1, "start"),
        new JobStatusCdc(Op.U, 0, "stop"),
        new JobStatusCdc(Op.I, 2, "start"),
        new JobStatusCdc(Op.D, 0, null),
        new JobStatusCdc(Op.U, 1, "stop"));

    // Once job is completed, you should see two records -
    /*
    2	start
    1	stop
    */
  }

  private static RowKind rowKind(Op op) {
    switch (op) {
      case I:
        return RowKind.INSERT;
      case U:
        return RowKind.UPDATE_AFTER;
      case D:
        return RowKind.DELETE;
    }
    throw new IllegalArgumentException("Unknown op " + op.name());
  }

  public static final class JobStatusCdc {
    Op op;
    int id;
    String status;

    public JobStatusCdc(Op op, int id, String status) {
      this.op = op;
      this.id = id;
      this.status = status;
    }
  }

  public enum Op {
    I, U, D
  }

}
