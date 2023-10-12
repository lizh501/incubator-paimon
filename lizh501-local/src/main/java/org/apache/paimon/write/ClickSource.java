package org.apache.paimon.write;


import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigInteger;

/**
 * @Author lizh501
 * @Date 2023/10/9 15:01
 * @Description
 */
public class ClickSource implements SourceFunction<Row> {
    private Boolean running = true;


    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        long l = 202310091512L;
        int i = 0;
        while (running) {

            Row row = Row.withPositions(5);
            row.setField(0, "1");
            row.setField(1, "zhangsan");
            row.setField(2, i+=1);
            row.setField(3, "male");
            row.setField(4,  l+=1);
            row.setKind(RowKind.INSERT);
            ctx.collect(row);
//            break;
            Thread.sleep(60000L);
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
