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
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

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
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

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

        // Set up streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        
        // Set up Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:29092");
        properties.setProperty("group.id", "flink-group");
        
        // Create a Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("processed-topic", new SimpleStringSchema(), properties);
        
        // Create a stream of custom elements and apply transformations
        DataStream<Tuple4<Object, Object, Object, Object>> source = env.addSource(consumer).map(new JsonToTuple<>());

        DataStream<Tuple5<Object, Object, Object, Object, Object>> dataStream = source
        .keyBy(tuple -> tuple.getField(0))
        .connect(ruleBroadcastStream)
        .process(new CastleFunction<Long, Tuple4<Object, Object, Object, Object>, Tuple5<Object, Object, Object, Object, Object>>(
            0, k, l, delta, beta, zeta, mu, true, 0, rules))
        .name(evalDescription);

        // Create a Kafka sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers("kafka:29092")
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic("prink-topic")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build())
        .build();

        // Add the sink to the data stream
        dataStream.map(tuple -> tuple.toString()).sinkTo(sink);

        // Execute the transformation pipeline
        return env.execute(evalDescription);
    }

    public enum DatasetFields {
        BPS(new AggregationIntegerGeneralizer(Tuple2.of(0, 3)), true),
        PULSE(new AggregationIntegerGeneralizer(Tuple2.of(0, 3)), true),
        TEMP(new AggregationIntegerGeneralizer(Tuple2.of(0, 3)), true),
        RESP(new AggregationIntegerGeneralizer(Tuple2.of(0, 3)), true)
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

    // Split incoming JSON object into Tuples
    public static class JsonToTuple<T extends Tuple> implements MapFunction<String, T> {
            
        @Override
                public T map(String s) throws Exception {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(s);
            T newTuple = (T) Tuple.newInstance(18);
            newTuple.setField(jsonNode.get("bps").asInt(), 0);
            newTuple.setField(jsonNode.get("pulse").asInt(), 1);
            newTuple.setField(jsonNode.get("temp").asInt(), 2);
            newTuple.setField(jsonNode.get("resp").asInt(), 3);
            return newTuple;
        }
    }

    // Turn Tuple into JSON object
    public static class TupleToJson<T extends Tuple> implements MapFunction<T, String> {
        
        @Override
               public String map(T t) throws Exception {
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode outputJson = objectMapper.createObjectNode();
            outputJson.put("bps", (String) t.getField(0));
            outputJson.put("pulse", (String) t.getField(1));
            outputJson.put("temp", (String) t.getField(2));
            outputJson.put("resp", (String) t.getField(3));
            return outputJson.toString();
        }
    }

}
