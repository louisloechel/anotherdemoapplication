package ganges;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prink.CastleFunction;
import prink.datatypes.CastleRule;
import prink.generalizations.*;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class GangesEvaluation {

    int k = 5;
    int l = 2;
    int delta = 200;
    int beta = 50;
    int zeta = 10;
    int mu = 10;

    private final static Logger LOG = LoggerFactory.getLogger(GangesEvaluation.class);

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        GangesEvaluation gangesEvaluation = new GangesEvaluation();
        JobExecutionResult r = gangesEvaluation.execute(params);
        System.out.println(r.toString());
    }

    public JobExecutionResult execute(ParameterTool parameters) throws Exception {

         // process provided job parameters
        k = parameters.getInt("k", k);
        l = parameters.getInt("l", l);
        delta = parameters.getInt("delta", delta);
        beta = parameters.getInt("beta", beta);
        zeta = parameters.getInt("zeta", zeta);
        mu = parameters.getInt("mu", mu);

        String sutHost = parameters.get("sut_host", "localhost");
        int sutPortWrite = parameters.getInt("sut_port_write", 50051);
        int sutPortRead = parameters.getInt("sut_port_read", 50052);

        // Set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "RulesBroadcastState",
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CastleRule>() {
                        }));

        List<CastleRule> rules = Arrays.stream(DatasetFields.values()).map(f -> new CastleRule(f.getId(), f.getGeneralizer(), f.isSensitive())).collect(Collectors.toList());

        BroadcastStream<CastleRule> ruleBroadcastStream = env.fromCollection(rules)
                .broadcast(ruleStateDescriptor);

        String evalDescription = String.format("k%d_delta%d_l%d_beta%d_zeta%d_mu%d", k, delta, l, beta, zeta, mu);

        SingleOutputStreamOperator<Tuple18<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> source = env.socketTextStream(sutHost, sutPortWrite)
                .map(new StringToTuple<>());
        // Create a stream of custom elements and apply transformations
        DataStream<Tuple19<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> dataStream = source
                .returns(TypeInformation.of(new TypeHint<Tuple18<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>>() {
                }))
                .keyBy(tuple -> tuple.getField(0))
                .connect(ruleBroadcastStream)
                .process(new CastleFunction<Long, Tuple18<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>, Tuple19<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>>(0, k, l, delta, beta, zeta, mu, true, 0, rules))
                .returns(TypeInformation.of(new TypeHint<Tuple19<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>>() {
                }))
                .name(evalDescription);

        dataStream.writeToSocket(sutHost, sutPortRead, new TupleToString<>()).setParallelism(1);

        // Execute the transformation pipeline
        return env.execute(evalDescription);
    }


    public enum DatasetFields {
        BUILDING_ID,
        TIMESTAMP,
        METER_READING(new AggregationFloatGeneralizer(Tuple2.of(0f, 100000f)), true),
        PRIMARY_USE(new NonNumericalGeneralizer(new String[][]{ {"private", "Lodging/Residential"}, {"public", "commercial", "Entertainment", "Technology/Science", "Office", "Parking"}, {"public", "administrative", "Education"}, {"public", "administrative", "Public Services"}, {"public", "administrative", "Utility"} }), false),
        SQUARE_FEET(new AggregationFloatGeneralizer(Tuple2.of(0f, 50f)), false),
        YEAR_BUILD(new AggregationFloatGeneralizer(Tuple2.of(1900f, 2020f)), false),
        FLOOR_COUNT(new AggregationFloatGeneralizer(Tuple2.of(0f, 15f)), false),
        AIR_TEMPERATURE(new AggregationFloatGeneralizer(Tuple2.of(1f, 1f)), false),
        CLOUD_COVERAGE(new AggregationFloatGeneralizer(Tuple2.of(0f, 9f)), false),
        DEW_TEMPERATURE(new AggregationFloatGeneralizer(Tuple2.of(1f, 1f)), false),
        PERCIP_DEPTH_1_HR,
        SEA_LEVEL_PRESSURE(new AggregationFloatGeneralizer(Tuple2.of(1f, 1f)), false),
        WIND_DIRECTION(new AggregationFloatGeneralizer(Tuple2.of(1f, 1f)), false),
        WIND_SPEED(new AggregationFloatGeneralizer(Tuple2.of(1f, 1f)), false),
        BUILDING_ID2,
        UNIXTIMESTAMP(new AggregationFloatGeneralizer(Tuple2.of(1451600000000000000f, 1490000000000000000f)), false),
        // message id from client
        M_ID,
        // ingestion timestamp
        TS,
        ;

        private final BaseGeneralizer generalizer;
        private final boolean sensitive;

        DatasetFields(BaseGeneralizer generalizer, boolean sensitive) {
            this.generalizer = generalizer;
            this.sensitive = sensitive;
        }

        DatasetFields() {
            this(new NoneGeneralizer(), false);
        }

        public int getId() {
            return this.ordinal();
        }

        public boolean isSensitive() {
            return sensitive;
        }

        public BaseGeneralizer getGeneralizer() {
            return generalizer;
        }


        public Object parse(String input) {
            if (this.generalizer instanceof AggregationFloatGeneralizer) {
                return Float.parseFloat(input);
            }
            if (this.generalizer instanceof AggregationIntegerGeneralizer) {
                return Integer.parseInt(input);
            }

            try {
                return Long.parseLong(input);
            } catch (Exception e) {
                return input;
            }
        }
    }

    public static class TupleToString<T extends Tuple> implements SerializationSchema<T> {

        @Override
        public byte[] serialize(T element) {
            StringWriter writer = new StringWriter();
            for (int i = 0 ; i < element.getArity() ; i++) {
                String sub = StringUtils.arrayAwareToString(element.getField(i));
                writer.write(sub);
                if (i + 1 < element.getArity()) {
                    writer.write(";");
                }
            }
            writer.write("\n");
            return writer.toString().getBytes(StandardCharsets.UTF_8);
        }
    }


    public static class StringToTuple<T extends Tuple> implements MapFunction<String, T> {

        @Override
        public T map(String s) throws Exception {
            String[] split = s.split(";");
            DatasetFields[] fields = DatasetFields.values();
            T newTuple = (T) Tuple.newInstance(fields.length);

            for (DatasetFields field : DatasetFields.values()) {
                if (split.length <= field.getId()) {
                    continue;
                }
                String input = split[field.getId()];
                try {
                    Object value = field.parse(input);
                    newTuple.setField(value, field.getId());
                } catch (Exception e) {
                    System.err.printf("could not parse field %s: %s (%s): %s %n", field.name(), input, s, e);
                }
            }
            return newTuple;
        }
    }

}
