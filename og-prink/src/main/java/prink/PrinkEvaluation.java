package prink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import prink.datatypes.CastleRule;
import prink.generalizations.AggregationFloatGeneralizer;
import prink.generalizations.NonNumericalGeneralizer;
import prink.generalizations.NoneGeneralizer;

import javax.swing.*;
import java.awt.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Class to test out prink functionality
 */
public class PrinkEvaluation {

    int k = 5;
    int l = 2;
    int delta = 200;
    int beta = 50;
    int zeta = 10;
    int mu = 10;

    ParameterTool parameters;

    /** Creates a job using the source and sink provided. */
    public PrinkEvaluation(ParameterTool parameters) {
        this.parameters = parameters;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        PrinkEvaluation job = new PrinkEvaluation(parameters);

        JobExecutionResult result = job.execute();
        System.out.println(result.toString());
    }

    /**
     * Create and execute the information reduction pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // process provided job parameters
        k = parameters.getInt("k", k);
        l = parameters.getInt("l", l);
        delta = parameters.getInt("delta", delta);
        beta = parameters.getInt("beta", beta);
        zeta = parameters.getInt("zeta", zeta);
        mu = parameters.getInt("mu", mu); // TODO is wrong configured in testing job (accepted "k" not "mu")
        final int ruleSet = parameters.getInt("ruleSet", 0);
        final boolean parallel = parameters.getBoolean("parallel", false); // false
        final boolean local = parameters.getBoolean("local", true); // true

        // ------------ DGH section ------------
        // [3, 10] workclass: Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked
        ArrayList<String[]> treeWorkclass = new ArrayList<>();
        treeWorkclass.add(new String[]{"Never-worked"});
        treeWorkclass.add(new String[]{"Self-emp","Self-emp-not-inc"});
        treeWorkclass.add(new String[]{"Self-emp","Self-emp-inc"});
        treeWorkclass.add(new String[]{"gov","Federal-gov"});
        treeWorkclass.add(new String[]{"gov","Local-gov"});
        treeWorkclass.add(new String[]{"gov","State-gov"});
        treeWorkclass.add(new String[]{"Without-pay"});
        treeWorkclass.add(new String[]{"Private"});

        // [5,26] education: Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool
        ArrayList<String[]> treeEducation = new ArrayList<>();
        treeEducation.add(new String[]{"Without-Post-Secondary","Preschool"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Elementary","1st-4th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Elementary","5th-6th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Elementary","7th-8th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Junior-Secondary","9th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Junior-Secondary","10th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Senior-Secondary","11th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Senior-Secondary","12th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Senior-Secondary","HS-grad"});
        treeEducation.add(new String[]{"Post-Secondary","Some-college"});
        treeEducation.add(new String[]{"Post-Secondary","Assoc","Assoc-acdm"});
        treeEducation.add(new String[]{"Post-Secondary","Assoc","Assoc-voc"});
        treeEducation.add(new String[]{"Post-Secondary","University","Bachelors"});
        treeEducation.add(new String[]{"Post-Secondary","University","Prof-school"});
        treeEducation.add(new String[]{"Post-Secondary","University","Post-grad","Masters"});
        treeEducation.add(new String[]{"Post-Secondary","University","Post-grad","Doctorate"});

        // [4,11] marital-status: Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse
        ArrayList<String[]> treeMarital = new ArrayList<>();
        treeMarital.add(new String[]{"married","has-spouse","Married-civ-spouse"});
        treeMarital.add(new String[]{"not-married","was-married","Divorced"});
        treeMarital.add(new String[]{"not-married","never-married","Never-married"});
        treeMarital.add(new String[]{"not-married","was-married","Separated"});
        treeMarital.add(new String[]{"married","alone","Widowed"});
        treeMarital.add(new String[]{"married","alone","Married-spouse-absent"});
        treeMarital.add(new String[]{"married","has-spouse","Married-AF-spouse"});

        // [3, 18] occupation: Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces
        ArrayList<String[]> treeOccupation = new ArrayList<>();
        treeOccupation.add(new String[]{"White-collar","Exec-managerial"});
        treeOccupation.add(new String[]{"White-collar","Prof-specialty"});
        treeOccupation.add(new String[]{"White-collar","Sales"});
        treeOccupation.add(new String[]{"White-collar","Adm-clerical"});
        treeOccupation.add(new String[]{"Blue-collar","Tech-support"});
        treeOccupation.add(new String[]{"Blue-collar","Craft-repair"});
        treeOccupation.add(new String[]{"Blue-collar","Machine-op-inspct"});
        treeOccupation.add(new String[]{"Blue-collar","Handlers-cleaners"});
        treeOccupation.add(new String[]{"Blue-collar","Priv-house-serv"});
        treeOccupation.add(new String[]{"Blue-collar","Transport-moving"});
        treeOccupation.add(new String[]{"Other","Protective-serv"});
        treeOccupation.add(new String[]{"Other","Armed-Forces"});
        treeOccupation.add(new String[]{"Other","Farming-fishing"});
        treeOccupation.add(new String[]{"Other","Other-service"});

        // [3, 9] relationship: Wife, Own-child, Husband, Not-in-family, Other-relative, Unmarried
        ArrayList<String[]> treeRelationship = new ArrayList<>();
        treeRelationship.add(new String[]{"Family","Wife"});
        treeRelationship.add(new String[]{"Family","Own-child"});
        treeRelationship.add(new String[]{"Family","Husband"});
        treeRelationship.add(new String[]{"Alone","Not-in-family"});
        treeRelationship.add(new String[]{"With-others","Other-relative"});
        treeRelationship.add(new String[]{"With-others","Unmarried"});

        // [3, 8] race: White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black
        ArrayList<String[]> treeRace = new ArrayList<>();
        treeRace.add(new String[]{"Group1","White"});
        treeRace.add(new String[]{"Group2","Asian-Pac-Islander"});
        treeRace.add(new String[]{"Group1","Amer-Indian-Eskimo"});
        treeRace.add(new String[]{"Other"});
        treeRace.add(new String[]{"Group2","Black"});

        // [2, 3] sex: Female, Male
        ArrayList<String[]> treeSex = new ArrayList<>();
        treeSex.add(new String[]{"Female"});
        treeSex.add(new String[]{"Male"});

        // [] nation: United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands
        ArrayList<String[]> treeNation = new ArrayList<>();
        treeNation.add(new String[]{"America", "North-America","United-States"});
        treeNation.add(new String[]{"America", "North-America","Canada"});

        treeNation.add(new String[]{"America", "Middle-America","Puerto-Rico"});
        treeNation.add(new String[]{"America", "Middle-America","Cuba"});
        treeNation.add(new String[]{"America", "Middle-America","Honduras"});
        treeNation.add(new String[]{"America", "Middle-America","Jamaica"});
        treeNation.add(new String[]{"America", "Middle-America","Mexico"});
        treeNation.add(new String[]{"America", "Middle-America","Dominican-Republic"});
        treeNation.add(new String[]{"America", "Middle-America","Haiti"});
        treeNation.add(new String[]{"America", "Middle-America","Guatemala"});
        treeNation.add(new String[]{"America", "Middle-America","Nicaragua"});
        treeNation.add(new String[]{"America", "Middle-America","El-Salvador"});
        treeNation.add(new String[]{"America", "Middle-America","Trinadad&Tobago"});

        treeNation.add(new String[]{"America", "South-America","Ecuador"});
        treeNation.add(new String[]{"America", "South-America","Columbia"});
        treeNation.add(new String[]{"America", "South-America","Peru"});

        treeNation.add(new String[]{"Asia", "South-East-Asia","Cambodia"});
        treeNation.add(new String[]{"Asia", "South-East-Asia","Philippines"});
        treeNation.add(new String[]{"Asia", "South-East-Asia","Vietnam"});
        treeNation.add(new String[]{"Asia", "South-East-Asia","Laos"});
        treeNation.add(new String[]{"Asia", "South-East-Asia","Thailand"});

        treeNation.add(new String[]{"Asia", "South-Asia","India"});

        treeNation.add(new String[]{"Asia", "East-Asia","Japan"});
        treeNation.add(new String[]{"Asia", "East-Asia","China"});
        treeNation.add(new String[]{"Asia", "East-Asia","Taiwan"});
        treeNation.add(new String[]{"Asia", "East-Asia","Hong"});
        treeNation.add(new String[]{"Asia", "East-Asia","South"});

        treeNation.add(new String[]{"Asia", "West-Asia","Iran"});

        treeNation.add(new String[]{"Europe", "South-Europe","Greece"});
        treeNation.add(new String[]{"Europe", "South-Europe","Italy"});
        treeNation.add(new String[]{"Europe", "South-Europe","Portugal"});

        treeNation.add(new String[]{"Europe", "West-Europe","England"});
        treeNation.add(new String[]{"Europe", "West-Europe","Germany"});
        treeNation.add(new String[]{"Europe", "West-Europe","Ireland"});
        treeNation.add(new String[]{"Europe", "West-Europe","France"});
        treeNation.add(new String[]{"Europe", "West-Europe","Scotland"});
        treeNation.add(new String[]{"Europe", "West-Europe","Holand-Netherlands"});

        treeNation.add(new String[]{"Europe", "East-Europe","Poland"});
        treeNation.add(new String[]{"Europe", "East-Europe","Hungary"});
        treeNation.add(new String[]{"Europe", "East-Europe","Yugoslavia"});

        treeNation.add(new String[]{"Outlying-US(Guam-USVI-etc)"});

        // Set up rules
        ArrayList<CastleRule> rules = new ArrayList<>();
        switch (ruleSet) {
            case 0:
                // Paper rule set
                rules.add(new CastleRule(0, new NoneGeneralizer(), false));                                                    // key
                rules.add(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(17f, 90f)), false));                        // age: continuous
                rules.add(new CastleRule(2, new NoneGeneralizer(), false));                                                    // placeholder time
                rules.add(new CastleRule(3, new NoneGeneralizer(), true));                                      // workclass
                rules.add(new CastleRule(4, new AggregationFloatGeneralizer(Tuple2.of(13492f, 1490400f)), false));                // final weight
                rules.add(new CastleRule(5, new NonNumericalGeneralizer(treeEducation.toArray(new String[][]{})),false));                              // education
                rules.add(new CastleRule(6, new NonNumericalGeneralizer(treeMarital.toArray(new String[][]{})), false));                               // marital-status
                rules.add(new CastleRule(7, new NonNumericalGeneralizer(treeOccupation.toArray(new String[][]{})), false));                            // occupation
                rules.add(new CastleRule(8, new NoneGeneralizer(), true));                                   // relationship
                rules.add(new CastleRule(9, new NoneGeneralizer(), false));                                          // race
                rules.add(new CastleRule(10, new NoneGeneralizer(), false));                                          // sex
                rules.add(new CastleRule(11, new AggregationFloatGeneralizer(Tuple2.of(0f, 99999f)), false));                     // capital-gain
                rules.add(new CastleRule(12, new AggregationFloatGeneralizer(Tuple2.of(0f, 4356f)), false));                      // capital-loss
                rules.add(new CastleRule(13, new AggregationFloatGeneralizer(Tuple2.of(1f, 99f)), false));                        // hours-per-week
                rules.add(new CastleRule(14, new NoneGeneralizer(), false));                               // native-country (treeNation)
                break;
//            case 1:
//                // Alternative rule set
//                rules.add(new CastleRule(0, CastleFunction.Generalization.NONE, false));
//                rules.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION, Tuple2.of(17f, 90f), false, 1.0)); // age
//                rules.add(new CastleRule(2, CastleFunction.Generalization.NONE, false));
//                rules.add(new CastleRule(3, CastleFunction.Generalization.NONNUMERICAL, treeWorkclass, false)); // workclass
//                rules.add(new CastleRule(4, CastleFunction.Generalization.NONE, false)); // final weight
//                rules.add(new CastleRule(5, CastleFunction.Generalization.NONNUMERICAL, treeEducation,false));
//                rules.add(new CastleRule(6, CastleFunction.Generalization.NONNUMERICAL, treeMarital, false, 1.0));
//                rules.add(new CastleRule(7, CastleFunction.Generalization.NONNUMERICAL, treeOccupation, false, 1.0));
//                rules.add(new CastleRule(8, CastleFunction.Generalization.NONNUMERICAL, treeRelationship, false, 1.0));
//                rules.add(new CastleRule(9, CastleFunction.Generalization.NONNUMERICAL, treeRace, false, 1.0));
//                rules.add(new CastleRule(10, CastleFunction.Generalization.NONNUMERICAL, treeSex, false, 1.0));
//                rules.add(new CastleRule(11, CastleFunction.Generalization.NONE, true));
//                rules.add(new CastleRule(12, CastleFunction.Generalization.NONE, true));
//                rules.add(new CastleRule(13, CastleFunction.Generalization.NONE, true));
//                rules.add(new CastleRule(14, CastleFunction.Generalization.NONE, false)); //NONNUMERICAL, false));
//                break;
//            case 2:
//                // Rules with info loss multiplier (high multiplier for non-numerical, low for aggregation)
//                rules.add(new CastleRule(0, CastleFunction.Generalization.NONE, false));
//                rules.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION, Tuple2.of(17f, 90f), false, 0.05)); // age
//                rules.add(new CastleRule(2, CastleFunction.Generalization.NONE, false));
//                rules.add(new CastleRule(3, CastleFunction.Generalization.NONE, treeWorkclass, true)); // workclass
//                rules.add(new CastleRule(4, CastleFunction.Generalization.AGGREGATION, Tuple2.of(13492f, 1490400f), false, 0.05)); // final weight
//                rules.add(new CastleRule(5, CastleFunction.Generalization.NONNUMERICAL, treeEducation,false, 0.2));
//                rules.add(new CastleRule(6, CastleFunction.Generalization.NONNUMERICAL, treeMarital, false, 0.2));
//                rules.add(new CastleRule(7, CastleFunction.Generalization.NONNUMERICAL, treeOccupation, false, 0.2));
//                rules.add(new CastleRule(8, CastleFunction.Generalization.NONE, treeRelationship, true)); // relationship
//                rules.add(new CastleRule(9, CastleFunction.Generalization.NONE, treeRace, false)); // race
//                rules.add(new CastleRule(10, CastleFunction.Generalization.NONE, treeSex, false)); // sex
//                rules.add(new CastleRule(11, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f, 99999f), false, 0.05)); // capital-gain
//                rules.add(new CastleRule(12, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f, 4356f), false, 0.05)); // capital-loss
//                rules.add(new CastleRule(13, CastleFunction.Generalization.AGGREGATION, Tuple2.of(1f, 99f), false, 0.05)); // hours per week
//                rules.add(new CastleRule(14, CastleFunction.Generalization.NONNUMERICAL, treeNation, false, 0.2)); // nation
////                rules.add(new CastleRule(15, CastleFunction.Generalization.NONE, false)); // visualizer placeholder blank
////                rules.add(new CastleRule(16, CastleFunction.Generalization.NONE, false)); // visualizer placeholder blank
//                break;
//            case 3:
//                // Rules with info loss multiplier (low multiplier for non-numerical, high for aggregation)
//                rules.add(new CastleRule(0, CastleFunction.Generalization.NONE, false));
//                rules.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION, Tuple2.of(17f, 90f), false, 0.16)); // age
//                rules.add(new CastleRule(2, CastleFunction.Generalization.NONE, false));
//                rules.add(new CastleRule(3, CastleFunction.Generalization.NONE, treeWorkclass, true)); // workclass
//                rules.add(new CastleRule(4, CastleFunction.Generalization.AGGREGATION, Tuple2.of(13492f, 1490400f), false, 0.16)); // final weight
//                rules.add(new CastleRule(5, CastleFunction.Generalization.NONNUMERICAL, treeEducation,false, 0.05));
//                rules.add(new CastleRule(6, CastleFunction.Generalization.NONNUMERICAL, treeMarital, false, 0.05));
//                rules.add(new CastleRule(7, CastleFunction.Generalization.NONNUMERICAL, treeOccupation, false, 0.05));
//                rules.add(new CastleRule(8, CastleFunction.Generalization.NONE, treeRelationship, true)); // relationship
//                rules.add(new CastleRule(9, CastleFunction.Generalization.NONE, treeRace, false)); // race
//                rules.add(new CastleRule(10, CastleFunction.Generalization.NONE, treeSex, false)); // sex
//                rules.add(new CastleRule(11, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f, 99999f), false, 0.16)); // capital-gain
//                rules.add(new CastleRule(12, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f, 4356f), false, 0.16)); // capital-loss
//                rules.add(new CastleRule(13, CastleFunction.Generalization.AGGREGATION, Tuple2.of(1f, 99f), false, 0.16)); // hours per week
//                rules.add(new CastleRule(14, CastleFunction.Generalization.NONNUMERICAL, treeNation, false, 0.05)); // nation
////                rules.add(new CastleRule(15, CastleFunction.Generalization.NONE, false)); // visualizer placeholder blank
////                rules.add(new CastleRule(16, CastleFunction.Generalization.NONE, false)); // visualizer placeholder blank
//                break;
//            case 4:
//                // Paper rule set, but with only one sensitive attribute
//                rules.add(new CastleRule(0, CastleFunction.Generalization.NONE, false));                                                    // key
//                rules.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION, Tuple2.of(17f, 90f), false));                        // age: continuous
//                rules.add(new CastleRule(2, CastleFunction.Generalization.NONE, false));                                                    // placeholder time
//                rules.add(new CastleRule(3, CastleFunction.Generalization.NONE, treeWorkclass, true));                                      // workclass
//                rules.add(new CastleRule(4, CastleFunction.Generalization.AGGREGATION, Tuple2.of(13492f, 1490400f), false));                // final weight
//                rules.add(new CastleRule(5, CastleFunction.Generalization.NONNUMERICAL, treeEducation,false));                              // education
//                rules.add(new CastleRule(6, CastleFunction.Generalization.NONNUMERICAL, treeMarital, false));                               // marital-status
//                rules.add(new CastleRule(7, CastleFunction.Generalization.NONNUMERICAL, treeOccupation, false));                            // occupation
//                rules.add(new CastleRule(8, CastleFunction.Generalization.NONE, treeRelationship, false));                                   // relationship
//                rules.add(new CastleRule(9, CastleFunction.Generalization.NONE, treeRace, false));                                          // race
//                rules.add(new CastleRule(10, CastleFunction.Generalization.NONE, treeSex, false));                                          // sex
//                rules.add(new CastleRule(11, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f, 99999f), false));                     // capital-gain
//                rules.add(new CastleRule(12, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f, 4356f), false));                      // capital-loss
//                rules.add(new CastleRule(13, CastleFunction.Generalization.AGGREGATION, Tuple2.of(1f, 99f), false));                        // hours-per-week
//                rules.add(new CastleRule(14, CastleFunction.Generalization.NONNUMERICAL, treeNation, false));                               // native-country
//                break;
        }

        // Set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple17<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> sourceData;
        if(local){
            final FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("C:/Users/Groneberg/Desktop/Studium/AAA_Master/5_Masterarbeit/Repos/prink_github/src/main/resources/adult_big_clean.csv")).build(); // adult_small_clean.csv")).build(); // adult_small_clean
//            final FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("adult_big_clean.csv")).build(); // adult_small_clean
            // Start the data generator and arrange for watermarking
            sourceData = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source").map(new StringToTuple());
        }else{
            DataStreamSource<String> gs_bucket_file = env.readTextFile("gs://prink/adult_small_clean.csv"); // ("https://storage.googleapis.com/prink/adult_small_clean.csv");//
            // Start the data generator and arrange for watermarking
            sourceData = gs_bucket_file.map(new StringToTuple());
        }

        MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "RulesBroadcastState",
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CastleRule>(){}));

        BroadcastStream<CastleRule> ruleBroadcastStream = env.fromCollection(rules)
                .broadcast(ruleStateDescriptor);

        String evalDescription = "Prink Eval: " + new SimpleDateFormat("yyyy-MM-dd hh-mm-ss").format(new Date()) + " (k=" + k + " l=" + l + " delta=" + delta + " beta=" + beta + " zeta=" + zeta + " mu=" + mu + " ruleSet=" + ruleSet + " parallel=" + parallel + ")";

        // Create a stream of custom elements and apply transformations
        DataStream<Tuple18<Object, Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> privateFares = sourceData
                .keyBy(tuple -> tuple.getField(0))
                .connect(ruleBroadcastStream)
                .process(new CastleFunction<Long, Tuple17<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>, Tuple18<Object, Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>>(0, k, l, delta, beta, zeta, mu, true, 1))
                .returns(TypeInformation.of(new TypeHint<Tuple18<Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object>>(){}))
                .map(new CalculatePerformance<>())
                .name(evalDescription);

        if(local) {
            PerformanceSink<Tuple18<Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> new_sink = new PerformanceSink<>();
            new_sink.setDescription(evalDescription);
            privateFares.addSink(new_sink);
        } else {
            if(parallel){
                privateFares.writeAsText("gs://prink");
            }else{
                privateFares.writeAsText("gs://prink/" + evalDescription+ ".txt");
            }
        }

        JobExecutionResult output = env.execute(evalDescription);

//        new BorderVisualiserV2().showVisuals(true);

        // Execute the transformation pipeline
        return output;
    }

    public static class StringToTuple implements MapFunction<String, Tuple17<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> {

        long counter = 0;

        @Override
        public Tuple17<Object, Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object> map(String input) {
            String[] splitValues = input.split(", ");
            counter++;
            if(splitValues.length >= 15) {
                // remove 'education-num' and add 'id' for user identification (expect every entry to be an individual)
                return Tuple17.of( counter, //ThreadLocalRandom.current().nextLong(100),
                        Float.parseFloat(splitValues[0]),   // age: continuous
                        null,                               // placeholder time
                        splitValues[1],                     // workclass: Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked.
                        Float.parseFloat(splitValues[2]),   // fnlwgt: continuous.
                        splitValues[3],                     // education: Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool.
//                        splitValues[4],                       // education-num: continuous.
                        splitValues[5],                     // marital-status: Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse.
                        splitValues[6],                     // occupation: Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces.
                        splitValues[7],                     // relationship: Wife, Own-child, Husband, Not-in-family, Other-relative, Unmarried.
                        splitValues[8],                     // race: White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black.
                        splitValues[9],                     // sex: Female, Male.
                        Float.parseFloat(splitValues[10]),  // capital-gain: continuous.
                        Float.parseFloat(splitValues[11]),  // capital-loss: continuous.
                        Float.parseFloat(splitValues[12]),  // hours-per-week: continuous.
                        splitValues[13],// );                   // native-country: United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands.
//                          splitValues[13]                     // label: >50K, <=50K
                        Float.parseFloat(splitValues[0]),  // capital-gain: continuous.
                        Float.parseFloat(splitValues[12]));  // capital-loss: continuous.
            }else{
                System.out.println("DEBUG:ERROR: could not load:" + input);
                return new Tuple17<>(counter, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
            }
        }
    }

    private static class PerformanceSink<INPUT extends Tuple> implements SinkFunction<INPUT> {

        private String description;
        public static final List<Tuple4<Float,Float,Tuple2<Float,Float>,Tuple2<Float,Float>>> tuplePoints = Collections.synchronizedList(new ArrayList<>());

        public void setDescription(String desc){
            this.description = desc;
        }

        @Override
        public void invoke(INPUT input, SinkFunction.Context context) {

            System.out.println(description + input);

            // Visualization
            if (input.getField(15) != null && input.getField(16) != null && input.getField(1) != null && input.getField(13) != null){
                tuplePoints.add(Tuple4.of(input.getField(15),input.getField(16), input.getField(1), input.getField(13)));
            }
        }
    }

    public static class CalculatePerformance<INPUT extends Tuple> implements MapFunction<INPUT, INPUT> {

        @Override
        public INPUT map(INPUT input) {
            // To use timer, enable time set inside CastleFunction (Field 2 of Tuple)
            long inputTime = System.currentTimeMillis();
            long startTime = input.getField(2) != null ? (long) input.getField(2) : 0L;
            if (input.getField(2) != null) input.setField(inputTime - startTime, 2) ;
            return input;
        }
    }

    /**
     * Base Code from: <a href="https://stackoverflow.com/questions/8693342/drawing-a-simple-line-graph-in-java">https://stackoverflow.com/questions/8693342/drawing-a-simple-line-graph-in-java</a>
     */
    public class BorderVisualiserV2 extends JPanel {

        private static final int padding = 25;
        private static final int labelPadding = 25;
        private final Color gridColor = new Color(200, 200, 200, 200);
        private final Stroke GRAPH_STROKE = new BasicStroke(0.5f);
        private static final int pointWidth = 6;
        private static final int numberYDivisions = 10;
        private static final int numberXDivisions = 10;

        int startShownTuples = 0;
        int endShownTuples = 0;

        HashMap<String, Color> borderColorMap = new HashMap<>();

        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            if(true) {
                Graphics2D g2 = (Graphics2D) g;
                g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

                double maxScoreX = getMaxScoreX();
                double maxScoreY = getMaxScoreY();
                double minScoreX = getMinScoreX();
                double minScoreY = getMinScoreY();

                double xScale = ((double) getWidth() - 2 * padding - labelPadding) / (maxScoreX - minScoreX);
                double yScale = ((double) getHeight() - 2 * padding - labelPadding) / (maxScoreY - minScoreY);

                //            List<Point> graphPoints = new ArrayList<>();
                //            for (int i = startShownTuples; i < endShownTuples; i++) { //PerformanceSink.tuplePoints.size(); i++) {
                //                int x1 = (int) ((maxScoreX - PerformanceSink.tuplePoints.get(i).f0) * xScale + padding + labelPadding); // (i * xScale + padding + labelPadding);
                //                int y1 = (int) ((maxScoreY - PerformanceSink.tuplePoints.get(i).f1) * yScale + padding);
                //                graphPoints.add(new Point(x1, y1));
                //            }

                // draw white background
                g2.setColor(Color.WHITE);
                g2.fillRect(padding + labelPadding, padding, getWidth() - (2 * padding) - labelPadding, getHeight() - 2 * padding - labelPadding);
                g2.setColor(Color.BLACK);

                // create hatch marks and grid lines for y-axis.
                for (int i = 0; i < numberYDivisions + 1; i++) {
                    int x0 = padding + labelPadding;
                    int x1 = pointWidth + padding + labelPadding;
                    int y0 = getHeight() - ((i * (getHeight() - padding * 2 - labelPadding)) / numberYDivisions + padding + labelPadding);
                    int y1 = y0;
                    if (PerformanceSink.tuplePoints.size() > 0) {
                        g2.setColor(gridColor);
                        g2.drawLine(padding + labelPadding + 1 + pointWidth, y0, getWidth() - padding, y1);
                        g2.setColor(Color.BLACK);
                        String yLabel = ((int) ((minScoreY + (maxScoreY - minScoreY) * ((i * 1.0) / numberYDivisions)) * 100)) / 100.0 + "";
                        FontMetrics metrics = g2.getFontMetrics();
                        int labelWidth = metrics.stringWidth(yLabel);
                        g2.drawString(yLabel, x0 - labelWidth - 5, y0 + (metrics.getHeight() / 2) - 3);
                    }
                    g2.drawLine(x0, y0, x1, y1);
                }

                // and for x-axis
                for (int i = 0; i < numberXDivisions; i++) {
                    int x0 = i * (getWidth() - padding * 2 - labelPadding) / numberXDivisions + padding + labelPadding;
                    int x1 = x0;
                    int y0 = getHeight() - padding - labelPadding;
                    int y1 = y0 - pointWidth;
                    if (PerformanceSink.tuplePoints.size() > 0) {
                        g2.setColor(gridColor);
                        g2.drawLine(x0, getHeight() - padding - labelPadding - 1 - pointWidth, x1, padding);
                        g2.setColor(Color.BLACK);
                        String xLabel = ((int) ((minScoreX + (maxScoreX - minScoreX) * ((i * 1.0) / numberXDivisions)) * 100)) / 100.0 + "";// i + "";
                        FontMetrics metrics = g2.getFontMetrics();
                        int labelWidth = metrics.stringWidth(xLabel);
                        g2.drawString(xLabel, x0 - labelWidth / 2, y0 + metrics.getHeight() + 3);
                    }
                    g2.drawLine(x0, y0, x1, y1);
                }

                // create x and y axes
                g2.drawLine(padding + labelPadding, getHeight() - padding - labelPadding, padding + labelPadding, padding);
                g2.drawLine(padding + labelPadding, getHeight() - padding - labelPadding, getWidth() - padding, getHeight() - padding - labelPadding);

                Stroke oldStroke = g2.getStroke();
                Random random = new Random();
                g2.setStroke(GRAPH_STROKE);
                for (int i = startShownTuples; i < endShownTuples; i++) { //PerformanceSink.tuplePoints.size(); i++) {

                    int x1 = (int) ((maxScoreX - PerformanceSink.tuplePoints.get(i).f0) * xScale + padding + labelPadding); // (i * xScale + padding + labelPadding);
                    int y1 = (int) ((maxScoreY - PerformanceSink.tuplePoints.get(i).f1) * yScale + padding);

                    Tuple2<Float, Float> border1 = PerformanceSink.tuplePoints.get(i).f2;
                    Tuple2<Float, Float> border2 = PerformanceSink.tuplePoints.get(i).f3;
                    int borderX1 = (int) ((maxScoreX - border1.f0) * xScale + padding + labelPadding);
                    int borderY1 = (int) ((maxScoreY - border2.f0) * yScale + padding);
                    int borderX2 = (int) ((maxScoreX - border1.f1) * xScale + padding + labelPadding);
                    int borderY2 = (int) ((maxScoreY - border2.f1) * yScale + padding);

                    g2.setColor(borderColorMap.computeIfAbsent(borderX1 + "-" + borderY1 + "-" + borderX2 + "-" + borderY2, (k) -> new Color(random.nextInt(255), random.nextInt(255), random.nextInt(255))));

                    g2.drawLine(borderX1, borderY1, borderX1, borderY2);
                    g2.drawLine(borderX1, borderY1, borderX2, borderY1);
                    g2.drawLine(borderX1, borderY2, borderX2, borderY2);
                    g2.drawLine(borderX2, borderY1, borderX2, borderY2);

                    g2.fillOval(x1 - pointWidth / 2, y1 - pointWidth / 2, pointWidth, pointWidth);
                }
                if (borderColorMap.size() > 1000) {
                    System.out.println("DEBUG: Border color map hash cleared. Value was over 1000.");
                    borderColorMap.clear();
                }
            } else {
//                BufferedImage bi = new BufferedImage(getWidth(), getHeight(), BufferedImage.TYPE_INT_ARGB);

                // Use nice display to look at
//                Graphics2D g2 = bi.createGraphics();//(Graphics2D) g;
                Graphics2D g2 = (Graphics2D) g;
                g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

                double maxScoreX = getMaxScoreX();
                double maxScoreY = getMaxScoreY();
                double minScoreX = getMinScoreX();
                double minScoreY = getMinScoreY();

                double xScale = ((double) getWidth() - 2 * padding - labelPadding) / (maxScoreX - minScoreX);
                double yScale = ((double) getHeight() - 2 * padding - labelPadding) / (maxScoreY - minScoreY);

                // draw white background
                g2.setColor(Color.BLACK);
                g2.fillRect(0, 0, getWidth(), getHeight());

                Random random = new Random();
                g2.setStroke(GRAPH_STROKE);
                for (int i = startShownTuples; i < endShownTuples; i++){

                    int x1 = (int) ((maxScoreX - PerformanceSink.tuplePoints.get(i).f0) * xScale + padding + labelPadding); // (i * xScale + padding + labelPadding);
                    int y1 = (int) ((maxScoreY - PerformanceSink.tuplePoints.get(i).f1) * yScale + padding);

                    Tuple2<Float,Float> border1 = PerformanceSink.tuplePoints.get(i).f2;
                    Tuple2<Float,Float> border2 = PerformanceSink.tuplePoints.get(i).f3;
                    int borderX1 = (int) ((maxScoreX - border1.f0) * xScale + padding + labelPadding);
                    int borderY1 = (int) ((maxScoreY - border2.f0) * yScale + padding);
                    int borderX2 = (int) ((maxScoreX - border1.f1) * xScale + padding + labelPadding);
                    int borderY2 = (int) ((maxScoreY - border2.f1) * yScale + padding);

                    g2.setColor(borderColorMap.computeIfAbsent(borderX1 + "-" + borderY1 + "-" + borderX2 + "-" + borderY2, (k) -> new Color(random.nextInt(255),random.nextInt(255),random.nextInt(255))));

                    g2.drawLine(borderX1, borderY1, borderX1, borderY2);
                    g2.drawLine(borderX1, borderY1, borderX2, borderY1);
                    g2.drawLine(borderX1, borderY2, borderX2, borderY2);
                    g2.drawLine(borderX2, borderY1, borderX2, borderY2);

                    g2.fillOval(x1 - pointWidth / 2, y1 - pointWidth / 2, pointWidth, pointWidth);
                }

//                try {
//                    ImageIO.write(bi, "PNG", new File("C:/Users/Philip/Desktop/Arbeit/PRINK/fancy_images/run_all_active_multiplier/slice_" + System.currentTimeMillis() + ".png"));
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
                if(borderColorMap.size() > 1000){
                    System.out.println("DEBUG: Border color map hash cleared. Value was over 1000.");
                    borderColorMap.clear();
                }
            }
        }

        /**
         * Creates a visual representation of the cluster creation of Prink
         * @param manualDisplay if true the display can be manual controlled
         */
        public void showVisuals(boolean manualDisplay) throws InterruptedException {
            setPreferredSize(new Dimension(1920, 1080));
            if(manualDisplay) {
                JSlider numOfTuplesShown = new JSlider(JSlider.VERTICAL, 0, PerformanceSink.tuplePoints.size(), 0);
                numOfTuplesShown.addChangeListener(e -> {
                    JSlider source = (JSlider) e.getSource();
                    if (!source.getValueIsAdjusting()) {
                        endShownTuples = source.getValue();
                        startShownTuples = Math.max(0, endShownTuples - delta);
                        repaint();
                    }
                });
                numOfTuplesShown.setMajorTickSpacing(PerformanceSink.tuplePoints.size() / 10);
//                numOfTuplesShown.setMinorTickSpacing(1);
                numOfTuplesShown.setPaintTicks(false);
                numOfTuplesShown.setPaintLabels(true);
                add(numOfTuplesShown);
            }

            JFrame frame2 = new JFrame("Cluster Enlargement Visuals (Borders are NOT dynamic! Ordered by Cluster OUTPUT time NOT by tuple input time!!!)");
            frame2.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
            frame2.getContentPane().add(this);
            frame2.pack();
            frame2.setLocationRelativeTo(null);
            frame2.setVisible(true);

            int counter = 0;
            while (!manualDisplay && counter < PerformanceSink.tuplePoints.size() && counter < 1000) {
                endShownTuples = counter;
                startShownTuples = Math.max(0, endShownTuples - delta);
                counter++;
                repaint();
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }

        private double getMinScoreX() {
            double minScore = Double.MAX_VALUE;
            for (Tuple4<Float, Float, Tuple2<Float, Float>, Tuple2<Float, Float>> tuple : PerformanceSink.tuplePoints) {
                minScore = Math.min(minScore, tuple.f0);
            }
            return minScore;
        }

        private double getMaxScoreX() {
            double maxScore = Double.MIN_VALUE;
            for (Tuple4<Float, Float, Tuple2<Float, Float>, Tuple2<Float, Float>> tuple : PerformanceSink.tuplePoints) {
                maxScore = Math.max(maxScore, tuple.f0);
            }
            return maxScore;
        }
        private double getMinScoreY() {
            double minScore = Double.MAX_VALUE;
            for (Tuple4<Float, Float, Tuple2<Float, Float>, Tuple2<Float, Float>> tuple : PerformanceSink.tuplePoints) {
                minScore = Math.min(minScore, tuple.f1);
            }
            return minScore;
        }

        private double getMaxScoreY() {
            double maxScore = Double.MIN_VALUE;
            for (Tuple4<Float, Float, Tuple2<Float, Float>, Tuple2<Float, Float>> tuple : PerformanceSink.tuplePoints) {
                maxScore = Math.max(maxScore, tuple.f1);
            }
            return maxScore;
        }

    }
}
