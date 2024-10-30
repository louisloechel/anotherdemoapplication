package prink.generalizations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import prink.datatypes.Cluster;

import java.util.List;

public class NoneGeneralizer implements BaseGeneralizer{

//    private final int position;
//    private final Cluster owningCluster;

    public NoneGeneralizer(){
//        this.position = -1;
//        this.owningCluster = new Cluster(null, 0, false);
    }

//    private NoneGeneralizer(int position, Cluster owningCluster) {
//        this.position = position;
//        this.owningCluster = owningCluster;
//    }

    @Override
    public NoneGeneralizer clone(int position, Cluster owningCluster) {
        return new NoneGeneralizer();
    }

    @Override
    public Tuple2<?, Float> generalize() {
        return null;
    }

    @Override
    public Tuple2<?, Float> generalize(List<Tuple> withTuples) {
        return null;
    }

    @Override
    public Tuple2<?, Float> generalizeMax() {
        return null;
    }

    @Override
    public void addTuple(Tuple newTuple) {

    }

    @Override
    public void removeTuple(Tuple removedTuple) {

    }
}
