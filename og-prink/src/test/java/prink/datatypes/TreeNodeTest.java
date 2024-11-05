package prink.datatypes;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TreeNodeTest {

    TreeNode subject;

    @BeforeEach
    void setUp() {
        subject = new TreeNode("<root>");
    }

    @Test
    void containsContent() {
        subject.addByName("leaf", false);
        String searchValue = "find me!";
        assertNull(subject.containsContent(searchValue));

        subject.addByName(searchValue, true);
        assertNotNull(subject.containsContent(searchValue));
        assertEquals(searchValue, subject.containsContent(searchValue).getContent());
    }

    @Test
    void addByArrayWithoutExistingValues() {
        String[] addValues = {"Parent", "Middle", "Child"};
        subject.addByArray(addValues,false);

        assertNotNull(subject.containsContent(addValues[0]));
        assertNotNull(subject.containsContent(addValues[1]));
        assertNotNull(subject.containsContent(addValues[2]));
        assertEquals(1,subject.numOfLeaves());
    }

    @Test
    void addByArrayWithExistingValues() {
        String[] addValues = {"Parent", "Middle", "Child"};
        subject.addByName(addValues[0],false);
        subject.addByArray(addValues,false);

        assertNotNull(subject.containsContent(addValues[0]));
        assertNotNull(subject.containsContent(addValues[1]));
        assertNotNull(subject.containsContent(addValues[2]));
        assertEquals(1,subject.numOfLeaves());
    }

    @Test
    void addByNameWithoutExistingNode() {
        String addValue = "Add Me!";
        subject.addByName(addValue, false);
        assertNotNull(subject.containsContent(addValue));
        assertEquals(addValue, subject.containsContent(addValue).getContent());
        assertEquals(1,subject.numOfLeaves());
    }

    @Test
    void addByNameWithExistingNode() {
        String addValue = "Add Me!";
        subject.addByName(addValue, false);
        subject.addByName(addValue, false);
        assertNotNull(subject.containsContent(addValue));
        assertEquals(addValue, subject.containsContent(addValue).getContent());
        assertEquals(1,subject.numOfLeaves());
    }

    @Test
    void numOfLeaves(){
        // root node is without children a leave node
        assertEquals(1,subject.numOfLeaves());

        subject.addByName("Leave1", false);
        assertEquals(1,subject.numOfLeaves());

        subject.addByName("Leave2", false);
        assertEquals(2,subject.numOfLeaves());

        subject.addByArray(new String[]{"Leave1", "Leave3"}, false);
        assertEquals(2,subject.numOfLeaves());

        subject.addByArray(new String[]{"Leave1", "Leave4", "Leave5"}, false);
        assertEquals(3,subject.numOfLeaves());
    }

    @Test
    void compareTo() {
        TreeNode compareNodeEqual = new TreeNode(subject.getContent());
        TreeNode compareNodeNotEqual = new TreeNode("Random Stuff");
        assertEquals(0,subject.compareTo(compareNodeEqual));
        assertNotEquals(0,subject.compareTo(compareNodeNotEqual));
    }

    @Test
    void getGeneralizationOneDimension() {
        subject.addByName("Black", false);
        subject.addByName("White", true);
        assertEquals(Tuple2.of("Black", 0.0f),subject.getGeneralization(false));
        assertEquals(Tuple2.of("<root>", 1.0f),subject.getGeneralization(true));

        subject.addByName("White", false);
        assertEquals(Tuple2.of("<root>", 1.0f),subject.getGeneralization(false));

        subject.addByName("White", false);
        assertEquals(Tuple2.of("<root>", 1.0f),subject.getGeneralization(false));
    }

    @Test
    void getGeneralizationTwoDimension() {
        subject.addByArray(new String[]{"Round", "Ball"}, false);
        subject.addByArray(new String[]{"NotRound", "Cube"}, true);
        assertEquals(Tuple2.of("Ball", 0.0f),subject.getGeneralization(false));
        assertEquals(Tuple2.of("<root>", 1.0f),subject.getGeneralization(true));

        subject.addByArray(new String[]{"Round", "Cylinder"}, false);
        assertEquals(Tuple2.of("Round", 0.5f),subject.getGeneralization(false));

        subject.addByArray(new String[]{"Round", "Cylinder"}, false);
        assertEquals(Tuple2.of("Round", 0.5f),subject.getGeneralization(false));

        subject.addByArray(new String[]{"NotRound", "Cube"}, false);
        assertEquals(Tuple2.of("<root>", 1.0f),subject.getGeneralization(false));
    }

    @Test
    void removeTempNodes() {
        subject.addByName("filler", true);
        subject.addByName("filler2", false);
        subject.removeTempNodes();
    }
}