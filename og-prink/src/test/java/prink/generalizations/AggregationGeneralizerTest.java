package prink.generalizations;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import prink.CastleFunction;
import prink.datatypes.CastleRule;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class AggregationGeneralizerTest {

    // FIXME rework tests with new Generalizer Structure
//    AggregationFloatGeneralizer subject;
//    Tuple2<Float,Float> baseBounds = Tuple2.of(0f,100f);
//
//    @BeforeEach
//    void setUp() {
//        ArrayList<CastleRule> rulesTemp = new ArrayList<>();
//        rulesTemp.add(new CastleRule(0, new ReductionGeneralizer(), false));
//        rulesTemp.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION, baseBounds,false));
//        rulesTemp.add(new CastleRule(2, new NoneGeneralizer(), false));
//        rulesTemp.add(new CastleRule(3, new NoneGeneralizer(), true));
//
//        CastleRule[] rules = rulesTemp.toArray(new CastleRule[0]);
//
//        subject = new AggregationFloatGeneralizer(rules);
//    }
//
//    @Nested
//    @DisplayName("No Borders provided")
//    class NoBordersTests {
//
//        @BeforeEach
//        void setUp() {
//            ArrayList<CastleRule> rulesTemp = new ArrayList<>();
//            rulesTemp.add(new CastleRule(0, new ReductionGeneralizer(), false));
//            rulesTemp.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION,false));
//            rulesTemp.add(new CastleRule(2, new NoneGeneralizer(), false));
//            rulesTemp.add(new CastleRule(3, new NoneGeneralizer(), true));
//
//            CastleRule[] rules = rulesTemp.toArray(new CastleRule[0]);
//            subject = new AggregationFloatGeneralizer(rules);
//        }
//
//        @Test
//        void generalize() {
//            subject.updateAggregationBounds(new Tuple4<>(1,10f,"test",150));
//            subject.updateAggregationBounds(new Tuple4<>(2,10f,"test",250));
//            assertEquals(Tuple2.of(Tuple2.of(10f,10f),0.0f), subject.generalize(1));
//
//            subject.updateAggregationBounds(new Tuple4<>(1,-100f, "test", 150));
//            subject.updateAggregationBounds(new Tuple4<>(2,1000f, "test", 250));
//            assertEquals(Tuple2.of(Tuple2.of(-100f,1000f),1.0f), subject.generalize(1));
//        }
//
//        @Test
//        void generalizeMax() {
//            assertEquals(Tuple2.of(Tuple2.of(null, null), 1.0f), subject.generalizeMax(1));
//        }
//
//        @Test
//        void updateAggregationBounds() {
//            subject.updateAggregationBounds(new Tuple4<>(1,10f, "test", 150));
//            subject.updateAggregationBounds(new Tuple4<>(2,20f, "test", 250));
//            assertEquals(Tuple2.of(10f,20f), subject.getDomainRange(1));
//
//            subject.updateAggregationBounds(new Tuple4<>(1,-50f, "test", 150));
//            subject.updateAggregationBounds(new Tuple4<>(2,1000.5f, "test", 250));
//            assertEquals(Tuple2.of(-50f,1000.5f), subject.getDomainRange(1));
//        }
//    }
//
//    @Test
//    void infoLoss() {
//        subject.updateAggregationBounds(new Tuple4<>(1, 50f, "test", 150));
//        assertEquals(Tuple2.of(Tuple2.of(50f, 50f), 0.0f), subject.generalize(1));
//
//        subject.updateAggregationBounds(new Tuple4<>(1, 25f, "test", 150));
//        assertEquals(Tuple2.of(Tuple2.of(25f, 50f), 0.25f), subject.generalize(1));
//
//        subject.updateAggregationBounds(new Tuple4<>(1, 0f, "test", 150));
//        assertEquals(Tuple2.of(Tuple2.of(0f, 50f), 0.5f), subject.generalize(1));
//
//        subject.updateAggregationBounds(new Tuple4<>(1, 100f, "test", 150));
//        assertEquals(Tuple2.of(Tuple2.of(0f, 100f), 1.0f), subject.generalize(1));
//    }
//
//    @Test
//    void generalize() {
//        subject.updateAggregationBounds(new Tuple4<>(1,10f,"test",150));
//        subject.updateAggregationBounds(new Tuple4<>(2,10f,"test",250));
//        assertEquals(Tuple2.of(Tuple2.of(10f,10f),0.0f), subject.generalize(1));
//
//        subject.updateAggregationBounds(new Tuple4<>(2,0f,"test",250));
//        subject.updateAggregationBounds(new Tuple4<>(2,100f,"test",250));
//        assertEquals(Tuple2.of(Tuple2.of(0f,100f),1.0f), subject.generalize(1));
//
//        subject.updateAggregationBounds(new Tuple4<>(1,-100f, "test", 150));
//        subject.updateAggregationBounds(new Tuple4<>(2,1000f, "test", 250));
//        assertEquals(Tuple2.of(Tuple2.of(-100f,1000f),1.0f), subject.generalize(1));
//    }
//
//    @Test
//    void generalizeWithTuples() {
//        subject.updateAggregationBounds(new Tuple4<>(1,10f,"test",150));
//        assertEquals(Tuple2.of(Tuple2.of(10f,10f),0.0f), subject.generalize(Arrays.asList(new Tuple4[]{new Tuple4<>(2, 10f, "test", 250)}),1));
//
//        subject.updateAggregationBounds(new Tuple4<>(2,0f,"test",250));
//        assertEquals(Tuple2.of(Tuple2.of(0f,100f),1.0f), subject.generalize(Arrays.asList(new Tuple4[]{new Tuple4<>(2, 100f, "test", 250)}),1));
//
//        subject.updateAggregationBounds(new Tuple4<>(2,0f,"test",250));
//        assertEquals(Tuple2.of(Tuple2.of(0f,200f),1.0f), subject.generalize(Arrays.asList(new Tuple4[]{new Tuple4<>(2, 200f, "test", 250)}),1));
//
//        subject.updateAggregationBounds(new Tuple4<>(2,1000f, "test", 250));
//        assertEquals(Tuple2.of(Tuple2.of(-100f,1000f),1.0f), subject.generalize(Arrays.asList(new Tuple4[]{new Tuple4<>(2, -100f, "test", 250)}),1));
//    }
//
//    @Test
//    void generalizeMax() {
//        assertEquals(Tuple2.of(baseBounds, 1.0f), subject.generalizeMax(1));
//    }
//
//    @Test
//    void updateAggregationBounds() {
//        subject.updateAggregationBounds(new Tuple4<>(1,10f, "test", 150));
//        subject.updateAggregationBounds(new Tuple4<>(2,20f, "test", 250));
//        assertEquals(baseBounds, subject.getDomainRange(1));
//
//        subject.updateAggregationBounds(new Tuple4<>(1,-50f, "test", 150));
//        subject.updateAggregationBounds(new Tuple4<>(2,1000.5f, "test", 250));
//        assertEquals(Tuple2.of(-50f,1000.5f), subject.getDomainRange(1));
//    }
//
//    @Test
//    void updateAggregationBoundsTypeInt() {
//        subject.updateAggregationBounds(new Tuple4<>(1,2000, "test", 150));
//        assertEquals(Tuple2.of(baseBounds.f0, 2000f), subject.getDomainRange(1));
//    }

}