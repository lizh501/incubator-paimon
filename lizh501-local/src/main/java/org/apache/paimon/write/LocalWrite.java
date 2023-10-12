package org.apache.paimon.write;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

/**
 * @Author lizh501
 * @Date 2023/10/9 15:00
 * @Description
 */
public class LocalWrite {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        //        conf.set(RestOptions.PORT, 8083);
        conf.set(RestOptions.BIND_PORT, "8081,8089");
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        HashMapStateBackend rocksDBStateBackend = new HashMapStateBackend();
        // 设置为机械硬盘+内存模式
//        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        bsEnv.setStateBackend(rocksDBStateBackend);
        bsEnv.enableCheckpointing(60000);

        // 配置 Checkpoint
        CheckpointConfig checkpointConf = bsEnv.getCheckpointConfig();
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        String checkpointPath =
                "file:///C:\\Users\\lizh501\\Desktop\\Work\\CUBD\\Code\\checkpoints\\paimon-0.6/";
        checkpointConf.setCheckpointStorage(checkpointPath);

        // 最小间隔 5s
        checkpointConf.setMinPauseBetweenCheckpoints(5000);
        // 超时时间 10分钟
        checkpointConf.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(10));
        // 保存checkpoint
        checkpointConf.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //
        // bsEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        StreamTableEnvironment env = StreamTableEnvironment.create(bsEnv);

        // access flink configuration after table environment instantiation
        TableConfig tableConfig = env.getConfig();
        // set low-level key-value options
        tableConfig.set("table.exec.sink.upsert-materialize", "NONE");


        env.executeSql(
                "CREATE CATALOG paimon_catalog WITH (\n"
                        + "    'type'='paimon',\n"
                        + "    'warehouse'='file:///C:/Users/lizh501/Desktop/Work/CUBD/Code/data/paimon/catalog-0.4'\n"
                        + ");");

        env.executeSql("use catalog paimon_catalog;");

        env.executeSql(
                "CREATE TABLE IF NOT EXISTS user_info_20231009(\n"
                        + " id STRING,\n"
                        + " name STRING,\n"
                        + " age INT,\n"
                        + " sex STRING,\n"
                        + " dt BIGINT,\n"
                        + " PRIMARY KEY (id) NOT ENFORCED \n"
                        + ") with (\n"
//                        + " 'log.consistency' = 'eventual',\n "
//                        + " 'write-only' = 'true',\n"
//                        + " 'merge-engine' = 'partial-update',\n"
//                        + " 'sequence.field' = 'dt',\n"
//                        + " 'changelog-producer' = 'lookup'\n"
                        + ");");

        //        env.executeSql("INSERT INTO user_info_20230627(id, name, dt) values('1',
        // 'zhangsan', 202306271539);");

//        env.executeSql("INSERT INTO user_info_20230627(id, age, dt) values('1', 20, 202306291719);");

//                env.executeSql("INSERT INTO user_info_20230627(id, sex, dt) values('1', 'male', 202307190109);");

//                env.executeSql("INSERT INTO user_info_20230627(id, age, dt) values('1', 23, 202308081712);");
        SingleOutputStreamOperator<Row> rowDataStreamSource = bsEnv.addSource(new ClickSource())
                .returns(Types.ROW_NAMED(
                        new String[]{
                                "id",
                                "name",
                                "age",
                                "sex",
                                "dt"
                        },
                        Types.STRING,
                        Types.STRING,
                        Types.INT,
                        Types.STRING,
                        Types.LONG
                ));
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.STRING())
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .column("sex", DataTypes.STRING())
                .column("dt", DataTypes.BIGINT())
                .build();
        Table table = env.fromChangelogStream(rowDataStreamSource, schema);

        env.createTemporaryView("InputTable", table);

        env.executeSql("INSERT INTO user_info_20231009 SELECT * FROM InputTable");

    }
}