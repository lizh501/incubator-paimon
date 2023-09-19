package org.apache.paimon.read;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Author lizh501
 * @Date 2023/7/19 0:38
 * @Description
 */
public class LocalRead {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        //        conf.set(RestOptions.PORT, 8082);
        conf.set(RestOptions.BIND_PORT, "8081,8089");
        // rocksdb设置
        conf.setString("state.backend.type", "rocksdb");
        conf.setString(
                "state.checkpoints.dir",
                "file:///C:\\Users\\lizh501\\Desktop\\Work\\CUBD\\Code\\checkpoints\\paimon-0.4/");
        conf.setString("state.backend.incremental", "true");
        conf.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        conf.setString(
                "execution.checkpointing.externalized-checkpoint-retention",
                "RETAIN_ON_CANCELLATION");
        conf.setString("execution.checkpointing.interval", "30000");

        conf.setString("table.exec.resource.default-parallelism", "1");

        // conf.setString("sql-client.execution.result-mode", "tableau");
        // conf.setString("execution.runtime-mode", "batch");

        conf.setString("table.exec.sink.upsert-materialize", "NONE");

        TableEnvironment env = TableEnvironment.create(conf);

        env.executeSql(
                "CREATE CATALOG paimon_catalog WITH (\n"
                        + "    'type'='paimon',\n"
                        + "    'warehouse'='file:///C:/Users/lizh501/Desktop/Work/CUBD/Code/data/paimon/catalog-0.4'\n"
                        + ");");

        env.executeSql("use catalog paimon_catalog;");

        env.executeSql(
                "CREATE TEMPORARY TABLE print_table (\n"
                        + "  id STRING,\n"
                        + "  name STRING,\n"
                        + "  age INT,\n"
                        + "  sex STRING,\n"
                        + "  dt BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ");");

        env.executeSql("insert into print_table select * from user_info_20230807;");
    }

}
