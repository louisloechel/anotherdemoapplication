package prink.datatypes;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import prink.generalizations.AggregationFloatGeneralizer;
import prink.generalizations.NoneGeneralizer;
import prink.generalizations.ReductionGeneralizer;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class ClusterTest {

    @BeforeEach
    void setUp() {
    }

    @Test
    void enlargementValueCluster() {
        CastleRule[] rules = new CastleRule[]{};
        Cluster subject = new Cluster(rules, 0,false);
        Cluster otherCluster = new Cluster(rules, 0,false);

        assertEquals(0.0f, subject.enlargementValue(otherCluster));
    }

    @Nested
    @DisplayName("Cluster with no entries")
    class NoEntriesTests {

        Cluster subject;

        @BeforeEach
        void setUp() {
            ArrayList<CastleRule> rulesTemp = new ArrayList<>();
            rulesTemp.add(new CastleRule(0, new ReductionGeneralizer(), false));
            rulesTemp.add(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(0f,0f)), false));
            rulesTemp.add(new CastleRule(2, new NoneGeneralizer(), false));
            rulesTemp.add(new CastleRule(3, new NoneGeneralizer(), true));

            CastleRule[] rules = rulesTemp.toArray(new CastleRule[0]);

            subject = new Cluster(rules, 0, false);
        }
        @Test
        void enlargementValueCluster() {
            CastleRule[] rules = new CastleRule[]{};
            Cluster otherCluster = new Cluster(rules, 0, false);
            assertEquals(0.0f, subject.enlargementValue(otherCluster));
        }

        @Test
        void enlargementValueTuple() {
            Tuple4<Integer, Float, String, Integer> tuple = new Tuple4<>(1, 2.5f, "Test", 10);
            assertEquals(0.0f, subject.enlargementValue(tuple));
        }

        @Test
        void informationLossWithCluster() {
            CastleRule[] rules = new CastleRule[]{};
            Cluster otherCluster = new Cluster(rules, 0, false);
            assertEquals(0.0f, subject.informationLossWith(otherCluster));
        }

        @Test
        void informationLossWithTuple() {
            Tuple4<Integer, Float, String, Integer> tuple = new Tuple4<>(1, 2.5f, "Test", 10);
            assertEquals(0.0f, subject.informationLossWith(tuple));
        }

        @Test
        void infoLoss() {
            assertEquals(0.0f, subject.infoLoss());
        }

        @Test
        void generalize() {
            Tuple4<Integer, Float, String, Integer> tuple = new Tuple4<>(1, 2.5f, "Test", 10);
            Tuple result = subject.generalize(tuple);
            assertAll("All values null",
                    () -> assertNull(result.getField(0)),
                    () -> assertNull(result.getField(1)),
                    () -> assertNull(result.getField(2)),
                    () -> assertNull(result.getField(3))
            );
        }

        @Test
        void generalizeMax() {
            Tuple4<Integer, Float, String, Integer> tuple = new Tuple4<>(1, 2.5f, "Test", 10);
            Tuple result = subject.generalize(tuple);
            assertAll("All values null",
                    () -> assertNull(result.getField(0)),
                    () -> assertNull(result.getField(1)),
                    () -> assertNull(result.getField(2)),
                    () -> assertNull(result.getField(3))
            );
        }

        @Test
        void contains() {
            Tuple4<Integer, Float, String, Integer> tuple = new Tuple4<>();
            assertFalse(subject.contains(tuple));
        }

        @Test
        void size() {
            assertEquals(0,subject.size());
        }

        @Test
        void diversity() {
            assertEquals(0,subject.diversity(new int[]{2}));
        }
    }

    @Test
    void infoLossWithOneEntry() {
        CastleRule[] rules = new CastleRule[]{};
        Cluster subject = new Cluster(rules, 0, false);
        subject.addEntry(new Tuple4<>(1, 10f, "Test", "Test"));
        assertEquals(0.0f, subject.infoLoss());
    }

}