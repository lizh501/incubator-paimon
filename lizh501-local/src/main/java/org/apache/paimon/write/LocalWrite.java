package org.apache.paimon.write;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @Author lizh501
 * @Date 2023/7/19 0:38
 * @Description
 */
public class LocalWrite {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        //        conf.set(RestOptions.PORT, 8083);
        conf.set(RestOptions.BIND_PORT, "8081,8089");
        StreamExecutionEnvironment bsEnv =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        // 设置为机械硬盘+内存模式
        rocksDBStateBackend.setPredefinedOptions(
                PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        bsEnv.setStateBackend(rocksDBStateBackend);
        bsEnv.enableCheckpointing(30000);

        // 配置 Checkpoint
        CheckpointConfig checkpointConf = bsEnv.getCheckpointConfig();
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        String checkpointPath =
                "file:///C:\\Users\\lizh501\\Desktop\\Work\\CUBD\\Code\\checkpoints\\paimon-0.4/";
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

        env.executeSql(
                "CREATE CATALOG paimon_catalog WITH (\n"
                        + "    'type'='paimon',\n"
                        + "    'warehouse'='file:///C:/Users/lizh501/Desktop/Work/CUBD/Code/data/paimon/catalog-0.4'\n"
                        + ");");

        env.executeSql("use catalog paimon_catalog;");

        env.executeSql(
                "CREATE TABLE IF NOT EXISTS user_info_20230627(\n"
                        + " id STRING,\n"
                        + " name STRING,\n"
                        + " age INT,\n"
                        + " sex STRING,\n"
                        + " dt BIGINT,\n"
                        + " PRIMARY KEY (id) NOT ENFORCED \n"
                        + ") with (\n"
                        + " 'merge-engine' = 'partial-update',\n"
                        + " 'sequence.field' = 'dt',\n"
                        + " 'changelog-producer' = 'lookup'\n"
                        + ");");

        //        env.executeSql("INSERT INTO user_info_20230627(id, name, dt) values('1',
        // 'zhangsan', 202306271539);");

//        env.executeSql("INSERT INTO user_info_20230627(id, age, dt) values('1', 20, 202306291719);");

                env.executeSql("INSERT INTO user_info_20230627(id, sex, dt) values('1', 'male', 202307190109);");

        //        env.executeSql("INSERT INTO user_info_20230627(id, age, dt) values('1', 22,
        // 202306021439);");

    }
}
