package prink.generalizations;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import prink.CastleFunction;
import prink.datatypes.CastleRule;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class NonNumericalGeneralizerTest {

    // FIXME rework tests with new Generalizer Structure
//    NonNumericalGeneralizer subject;
//
//    @BeforeEach
//    void setUp() {
//        ArrayList<String[]> baseTree = new ArrayList<>();
//        baseTree.add(new String[]{"Node 1-3","Leave1"});
//        baseTree.add(new String[]{"Node 1-3","Leave2"});
//        baseTree.add(new String[]{"Node 1-3","Leave3"});
//        baseTree.add(new String[]{"Node 4-6","Leave4"});
//        baseTree.add(new String[]{"Node 4-6","Leave5"});
//        baseTree.add(new String[]{"Node 4-6","Leave6"});
//
//        ArrayList<CastleRule> rulesTemp = new ArrayList<>();
//        rulesTemp.add(new CastleRule(0, CastleFunction.Generalization.REDUCTION, false));
//        rulesTemp.add(new CastleRule(1, CastleFunction.Generalization.NONNUMERICAL, baseTree, false));
//        rulesTemp.add(new CastleRule(2, CastleFunction.Generalization.NONE, false));
//        rulesTemp.add(new CastleRule(3, CastleFunction.Generalization.NONE, true));
//
//        CastleRule[] rules = rulesTemp.toArray(new CastleRule[0]);
//
//        subject = new NonNumericalGeneralizer(rules);
//    }
//
//    @Test
//    void generalizeForWrongIndex() {
//        int pos = 0;
//        assertEquals(Tuple2.of("ERROR: Missing tree for position:" + pos, 1.0f), subject.generalize(pos));
//    }
//
//    @Test
//    void generalizeWithOneTuples() {
//        subject.updateTree(Tuple4.of(1,"Leave3", 10, 10));
//        assertEquals(Tuple2.of("Leave3", 0.0f), subject.generalize(1));
//    }
//
//    @Test
//    void generalizeWithMultipleTuples() {
//        subject.updateTree(Tuple4.of(1,"Leave3", 10, 10));
//        subject.updateTree(Tuple4.of(2,"Leave3", 20, 20));
//        assertEquals(Tuple2.of("Leave3", 0.0f), subject.generalize(1));
//
//        subject.updateTree(Tuple4.of(1,"Leave1", 10, 10));
//        assertEquals(Tuple2.of("Node 1-3", 0.4f), subject.generalize(1));
//
//        subject.updateTree(Tuple4.of(1,"Leave2", 10, 10));
//        assertEquals(Tuple2.of("Node 1-3", 0.4f), subject.generalize(1));
//
//        subject.updateTree(Tuple4.of(1,"Leave6", 10, 10));
//        assertEquals(Tuple2.of("<blank>", 1.0f), subject.generalize(1));
//    }
//
//    @Test
//    void generalizeWith() {
//        subject.updateTree(Tuple4.of(1,"Leave3", 10, 10));
//        assertEquals(Tuple2.of("Leave3", 0.0f), subject.generalize(Arrays.asList(new Tuple4[]{Tuple4.of(2,"Leave3", 20, 20)}),1));
//        assertEquals(Tuple2.of("Node 1-3", 0.4f), subject.generalize(Arrays.asList(new Tuple4[]{Tuple4.of(2,"Leave1", 20, 20)}),1));
//        assertEquals(Tuple2.of("<blank>", 1.0f), subject.generalize(Arrays.asList(new Tuple4[]{Tuple4.of(2,"Leave6", 20, 20)}),1));
//    }
//
//    @Test
//    void generalizeMax() {
//        assertEquals(Tuple2.of("<blank>",1.0f), subject.generalizeMax(1));
//    }
//
//    @Test
//    void updateTreeWithString() {
//        subject.updateTree(Tuple4.of(1,"Leave1", 10, 10));
//        assertEquals(Tuple2.of("Leave1",0.0f), subject.generalize(1));
//    }
//
//    @Test
//    void updateTreeWithArray() {
//        subject.updateTree(Tuple4.of(1,new String[]{"Node 1-3", "Leave1"}, 10, 10));
//        assertEquals(Tuple2.of("Leave1",0.0f), subject.generalize(1));
//        subject.updateTree(Tuple4.of(1,new String[]{"Node 1-3", "Leave2.5"}, 10, 10));
//        assertEquals(Tuple2.of("Node 1-3",0.5f), subject.generalize(1));
//    }
}