package prink.generalizations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import prink.datatypes.Cluster;

import java.util.List;

public class AggregationDoubleGeneralizer implements BaseGeneralizer{

    private final int position;
    private final Cluster owningCluster;

    private Tuple2<Double, Double> aggregationRange;
    private final Tuple2<Double, Double> domainRange;

    public AggregationDoubleGeneralizer(Tuple2<Double, Double> domainRange){
        this.domainRange = domainRange;
        this.position = -1;
        this.owningCluster = null;
    }

    private AggregationDoubleGeneralizer(Tuple2<Double, Double> domainRange, int position, Cluster owningCluster){
        this.domainRange = domainRange;
        this.position = position;
        this.owningCluster = owningCluster;
    }

    @Override
    public AggregationDoubleGeneralizer clone(int position, Cluster owningCluster) {
        return new AggregationDoubleGeneralizer(domainRange, position, owningCluster);
    }

    /**
     * Returns the previous recorded Aggregation bounds and the produced information loss
     * @return A <code>Tuple2</code> with <code>[Aggregation bounds as Tuple2<Double, Double>, InfoLoss as Float]</code>
     */
    @Override
    public Tuple2<Tuple2<Double, Double>, Float> generalize() {
        return Tuple2.of(aggregationRange, infoLoss(aggregationRange.f0, aggregationRange.f1));
    }

    /**
     * Returns the previous recorded Aggregation bounds, while considering the additional data tuples and the produced information loss
     * @param withTuples Tuples to consider when calculating the generalization
     * @return A <code>Tuple2</code> with <code>[Aggregation bounds as Tuple2<Double, Double>, InfoLoss as Float]</code>
     */
    @Override
    public Tuple2<Tuple2<Double, Double>, Float> generalize(List<Tuple> withTuples) {
        Double min = aggregationRange.f0;
        Double max = aggregationRange.f1;

        for(Tuple input: withTuples){
            min = Math.min(input.getField(position), min);
            max = Math.max(input.getField(position), max);
        }
        Tuple2<Double, Double> borders = Tuple2.of(min, max);
        return Tuple2.of(borders, infoLoss(min, max));
    }

    /**
     * Returns the maximal generalized value (the generalization with the highest information loss)
     * This function is used when a data tuple needs to be suppressed and the corresponding suppression methods is selected
     * @see prink.CastleFunction for more information on suppression methods
     * @return Tuple2<Generalized Value as Tuple2 of lower bound and upper bound, Information Loss>
     */
    @Override
    public Tuple2<Tuple2<Double, Double>, Float> generalizeMax() {
        return Tuple2.of(domainRange, 1.0f);
    }

    /**
     * Update the upper and lower bound of aggregated attribute with a newly added data tuple.<br>
     * (Should the new bounds exceed the domain range, it will be updated as well.)
     * @param newTuple the new added data tuple
     */
    @Override
    public void addTuple(Tuple newTuple) {
        // Get the new attribute value of the newTuple
        Double inputValue = newTuple.getField(position);

        // Initialize aggregation range if newTuple is the first tuple inside the cluster
        if(aggregationRange == null){
            aggregationRange = Tuple2.of(inputValue, inputValue);
            return;
        }

        // Calculate the new aggregation range
        aggregationRange.f0 = Math.min(inputValue, aggregationRange.f0);
        aggregationRange.f1 = Math.max(inputValue, aggregationRange.f1);

        // Update the domain range if needed
        domainRange.f0 = Math.min(aggregationRange.f0, domainRange.f0);
        domainRange.f1 = Math.max(aggregationRange.f1, domainRange.f1);
    }

    /**
     * Update the upper and lower bound of aggregated attribute considering that less data tuples are present.<br>
     * (The domain range will NOT be updated.)
     * @param removedTuple the removed data tuple
     */
    @SuppressWarnings("WrapperTypeMayBePrimitive")
    @Override
    public void removeTuple(Tuple removedTuple) {
        Double inputValue = removedTuple.getField(position);

        // If the removed tuple was part of the aggregation borders, recalculate the aggregation range
        if(inputValue.equals(aggregationRange.f0) || inputValue.equals(aggregationRange.f1)){
            Double min = Double.MAX_VALUE;
            Double max = Double.MIN_VALUE;

            for(Tuple clusterEntry: owningCluster.getAllEntries()){
                min = Math.min(clusterEntry.getField(position), min);
                max = Math.max(clusterEntry.getField(position), max);
            }
            aggregationRange.f0 = min;
            aggregationRange.f1 = max;
        }
    }

    /**
     * Calculate the information loss using the domain range and the min, max values of the generalization
     * @param min min value of generalization
     * @param max max value of generalization
     * @return information loss of the give position and values
     */
    private float infoLoss(double min, double max){
        if(min == max) return 0.0f;
        return Math.min((float)((max - min) / (domainRange.f1 - domainRange.f0)), 1.0f);
    }
}
