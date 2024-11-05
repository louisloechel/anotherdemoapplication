package examples;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import prink.CastleFunction;
import prink.datatypes.CastleRule;
import examples.datatypes.TaxiFare;
import examples.sources.TaxiFareGenerator;
import prink.generalizations.AggregationFloatGeneralizer;
import prink.generalizations.NonNumericalGeneralizer;
import prink.generalizations.NoneGeneralizer;
import prink.generalizations.ReductionGeneralizer;

import java.time.Instant;
import java.util.ArrayList;

/**
 * Class to test out flink functionality
 * This example uses the provided example classes from the Apache Flink examples
 */
public class PrinkInformationReduction {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple8<Object, Object, Object, Object,Object, Object, Object, Object>> sink;

    /** Creates a job using the source and sink provided. */
    public PrinkInformationReduction(SourceFunction<TaxiFare> source, SinkFunction<Tuple8<Object, Object, Object, Object,Object, Object, Object, Object>> sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        PrinkInformationReduction job = new PrinkInformationReduction(new TaxiFareGenerator(), new TimerSink());

        job.execute();
    }

    /**
     * Create and execute the information reduction pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // Set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Start the data generator and arrange for watermarking
        DataStream<Tuple8<Long, Long, Long, Instant, Object, Float, Float, Float>> fares = env
                .addSource(source)
                .assignTimestampsAndWatermarks(
                        // taxi fares are in order
                        WatermarkStrategy
                                .<TaxiFare>forMonotonousTimestamps()
                                .withTimestampAssigner((fare, t) -> fare.getEventTimeMillis()))
                .map(new TaxiFareToTuple());

        MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "RulesBroadcastState",
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CastleRule>(){}));

        // Create treeEntries for non-numerical generalizer
        ArrayList<String[]> treeEntries = new ArrayList<>();
        treeEntries.add(new String[]{"CARD","APPLE PAY"});
        treeEntries.add(new String[]{"CASH","Banknotes"});
        treeEntries.add(new String[]{"CASH","Coins"});
        treeEntries.add(new String[]{"CARD","CREDIT CARD"});

        treeEntries.add(new String[]{"CARD","MAESTRO CARD"});
        treeEntries.add(new String[]{"CARD","PAYPAL","Ratenzahlung"});
        treeEntries.add(new String[]{"CARD","PAYPAL","Auf Rechnung"});
        treeEntries.add(new String[]{"CARD","PAYPAL","Direktzahlung"});
        treeEntries.add(new String[]{"9-Euro Ticket","No payment"});

        // Broadcast the rules and create the broadcast state
        ArrayList<CastleRule> rules = new ArrayList<>();
//        rules.add(new CastleRule(0, CastleFunction.Generalization.NONE, false));
//        rules.add(new CastleRule(1, CastleFunction.Generalization.REDUCTION, false,0.8));
//        rules.add(new CastleRule(2, CastleFunction.Generalization.NONE, false));
//        rules.add(new CastleRule(3, CastleFunction.Generalization.NONE, false));
//        rules.add(new CastleRule(4, CastleFunction.Generalization.NONNUMERICAL, treeEntries, true, 0.1));
//        rules.add(new CastleRule(5, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f,100f), false, 0.1));
//        rules.add(new CastleRule(6, CastleFunction.Generalization.NONE, Tuple2.of(0f,200f), true));
//        rules.add(new CastleRule(7, CastleFunction.Generalization.NONE, Tuple2.of(10f,500f), true));

        rules.add(new CastleRule(0, new NoneGeneralizer(), false));
        rules.add(new CastleRule(1, new ReductionGeneralizer(), false,0.8));
        rules.add(new CastleRule(2, new NoneGeneralizer(), false));
        rules.add(new CastleRule(3, new NoneGeneralizer(), false));
        rules.add(new CastleRule(4, new NonNumericalGeneralizer(treeEntries.toArray(new String[][]{})), true, 0.1));
        rules.add(new CastleRule(5, new AggregationFloatGeneralizer(Tuple2.of(0f,100f)), false, 0.1));
        rules.add(new CastleRule(6, new NoneGeneralizer(), true));
        rules.add(new CastleRule(7, new NoneGeneralizer(), true));

        BroadcastStream<CastleRule> ruleBroadcastStream = env.fromCollection(rules)
                .broadcast(ruleStateDescriptor);

        DataStream<Tuple8<Object, Object, Object, Object,Object, Object, Object, Object>> privateFares = fares
                .keyBy(fare -> fare.getField(0))
                .connect(ruleBroadcastStream)
                .process(new CastleFunction())
//                .process(new CompareProcessingFunction())
                .returns(TypeInformation.of(new TypeHint<Tuple8<Object, Object, Object, Object,Object, Object, Object, Object>>(){}));

        privateFares.addSink(sink).name("Testing Sink");

        // execute the transformation pipeline
        return env.execute("Data Reduction Job (Prink)");
    }

    /**
     * Convert TaxiFares into a tuple8 representation
     */
    public static class TaxiFareToTuple implements MapFunction<TaxiFare, Tuple8<Long, Long, Long, Instant, Object, Float, Float, Float>> {

        @Override
        public Tuple8<Long, Long, Long, Instant, Object, Float, Float, Float> map(TaxiFare input) {
            return new Tuple8<>(input.rideId, input.taxiId, input.driverId, input.startTime,
                    input.paymentType, input.tip, input.tolls, input.totalFare);
        }
    }

    public static class TimerSink extends RichSinkFunction<Tuple8<Object, Object, Object, Object,Object, Object, Object, Object>> {

        @Override
        public void invoke(Tuple8<Object, Object, Object, Object,Object, Object, Object, Object> input, Context context) {

            long startTime = input.f2 != null ? (long) input.f2 : 0L;

            System.out.println(
                input +
                ";[Data Desc.]" +
                ";" + (context.currentProcessingTime() - startTime));
        }
    }
}
