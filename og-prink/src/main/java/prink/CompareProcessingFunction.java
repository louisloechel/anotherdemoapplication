package prink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class CompareProcessingFunction extends KeyedBroadcastProcessFunction {

    @Override
    public void processElement(Object input, ReadOnlyContext readOnlyContext, Collector collector) throws Exception {

        Tuple tuple = (Tuple) input;
        tuple.setField(Instant.now(), 2);

        collector.collect(input);
    }

    @Override
    public void processBroadcastElement(Object o, Context context, Collector collector) throws Exception {

    }
}
