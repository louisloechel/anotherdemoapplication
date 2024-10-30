package prink.generalizations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import prink.datatypes.Cluster;
import prink.datatypes.TreeNode;

import java.util.List;

public class NonNumericalGeneralizer implements BaseGeneralizer{

    private final int position;
//    private final Cluster owningCluster;

    private final String[][] treeDefinition;
    private final String rootName;
    private final TreeNode tree;

    /**
     * The constructor of the NonNumericalGeneralizer
     * @param treeDefinition the definition of the domain generalization tree
     * @param rootName the name of the root tree node
     */
    public NonNumericalGeneralizer(String[][] treeDefinition, String rootName){
        this.position = -1;
//        this.owningCluster = new Cluster(null, 0, false);

        this.rootName = rootName;
        this.treeDefinition = treeDefinition;
        // Do not initialize the tree since this one is only used as prototype, only store the the treeDefinition for the real tree initialization
        this.tree = new TreeNode(rootName);
    }

    /**
     * The constructor of the NonNumericalGeneralizer
     * @param treeDefinition the definition of the domain generalization tree
     */
    public NonNumericalGeneralizer(String[][] treeDefinition){
        this(treeDefinition, "<ROOT>");
    }

    private NonNumericalGeneralizer(String[][] treeDefinition, String rootName, int position){
        this.position = position;
//        this.owningCluster = owningCluster;

        this.rootName = rootName;
        this.treeDefinition = treeDefinition;
        this.tree = new TreeNode(rootName);

        // Initialize tree using treeDefinition
        for(String[] treeEntry: treeDefinition){
            tree.addByArray(treeEntry, true);
        }
        tree.removeTempNodes();
    }

    @Override
    public NonNumericalGeneralizer clone(int position, Cluster owningCluster) {
        return new NonNumericalGeneralizer(treeDefinition, rootName, position);
    }

    @Override
    public Tuple2<String, Float> generalize() {
        return tree.getGeneralization(false);
    }

    @Override
    public Tuple2<String, Float> generalize(List<Tuple> withTuples) {
        // Add withTuples temporary to the tree
        for(Tuple input: withTuples){
            if(input.getField(position).getClass().isArray()){
                tree.addByArray(input.getField(position), true);
            }else{
                tree.addByName(input.getField(position), true);
            }
        }
        // Calculate generalization
        Tuple2<String, Float> output = tree.getGeneralization(true);

        // Remove temporary nodes
        tree.removeTempNodes();

        return output;
    }

    @Override
    public Tuple2<String, Float> generalizeMax() {
        return Tuple2.of(rootName, 1.0f);
    }

    @Override
    public void addTuple(Tuple newTuple) {
        // Update the tree by adding new values to tree
        if(newTuple.getField(position).getClass().isArray()){
            tree.addByArray(newTuple.getField(position), false);
        }else{
            tree.addByName(newTuple.getField(position), false);
        }
    }

    /**
     * The removal of tuple is currently not supported for the NonNumericalGeneralizer
     * @param removedTuple the removed data tuple
     */
    @Override
    public void removeTuple(Tuple removedTuple) {

    }
}
