package prink.generalizations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import prink.datatypes.Cluster;

import java.util.ArrayList;
import java.util.List;

/**
 * The base interface for all generalizers used in Prink
 */
public interface BaseGeneralizer {
    /**
     * This method creates a new instance of this generalizer by cloning it. <br>
     * The method is called on cluster creation and includes some additional formation of the cluster that could be required by the generalizer
     * @param position the position of the attribute the generalizer needs to generalize
     * @param owningCluster the cluster owning the generalizer
     * @return new instance of the generalizer it was called on
     */
    BaseGeneralizer clone(int position, Cluster owningCluster);

    /**
     * Returns the generalized value and the resulting information loss from the generalization
     * @return Tuple2<Generalized Value, Information Loss>
     */
    Tuple2<?, Float> generalize();

    /**
     * Returns the generalized value (while using the provided extra tuples) and the resulting information loss from the generalization
     * @param withTuples data tuples to integrate into calculation
     * @return Tuple2<Generalized Value, Information Loss>
     */
    Tuple2<?, Float> generalize(List<Tuple> withTuples);

    /**
     * Returns the maximal generalized value (the generalization with the highest information loss)
     * This function is used when a data tuple needs to be suppressed and the corresponding suppression methods is selected
     * @see prink.CastleFunction for more information on suppression methods
     * @return Tuple2<Generalized Value, Information Loss>
     */
    Tuple2<?, Float> generalizeMax();

    /**
     * This method is called when a new data tuple is added to the cluster that owns this generalizer
     * @param newTuple the newly added data tuple
     */
    void addTuple(Tuple newTuple);

    /**
     * This method is called when a data tuple is removed from the cluster that owns this generalizer
     * @see prink.CastleFunction for more information on data tuple removal (data suppression and cluster re-use strategy)
     * @param removedTuple the removed data tuple
     */
    void removeTuple(Tuple removedTuple);
}
