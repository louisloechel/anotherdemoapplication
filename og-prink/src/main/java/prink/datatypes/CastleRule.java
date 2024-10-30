package prink.datatypes;

import org.apache.flink.api.java.tuple.Tuple2;
import prink.CastleFunction;
import prink.generalizations.BaseGeneralizer;

import java.util.ArrayList;

/**
 * Rule definition function used by Prink to configure rule set
 */
public class CastleRule {

    private final int position;
    private final BaseGeneralizer generalizer;
    private final boolean isSensibleAttribute;
    private final double infoLossMultiplier;

    /**
     * The CastleRule defines how an attribute position of the data tuples inside the stream should be generalized
     * @param position the position of the attribute inside the generalized data tuple
     * @param generalizer the generalizer that will be used for the generalization
     * @param isSensibleAttribute if the attribute position is a sensible attribute (used for l-diversity)
     * @param infoLossMultiplier the information loss multiplier of this attributes information loss used in the overall cluster information loss calculation (needs to be bigger than 0)
     */
    public CastleRule(int position, BaseGeneralizer generalizer, boolean isSensibleAttribute, double infoLossMultiplier){
        this.position = position;
        this.generalizer = generalizer;
        this.isSensibleAttribute = isSensibleAttribute;
        this.infoLossMultiplier = (infoLossMultiplier > 0) ? infoLossMultiplier : 0.0d;
    }

    /**
     * The CastleRule defines how an attribute position of the data tuples inside the stream should be generalized
     * @param position the position of the attribute inside the generalized data tuple
     * @param generalizer the generalizer that will be used for the generalization
     * @param isSensibleAttribute if the attribute position is a sensible attribute (used for l-diversity)
     */
    public CastleRule(int position, BaseGeneralizer generalizer, boolean isSensibleAttribute){
        this(position, generalizer, isSensibleAttribute, 0.0d);
    }

    public int getPosition() {
        return position;
    }

    public BaseGeneralizer getGeneralizer() {
        return generalizer;
    }

    public boolean getIsSensibleAttribute() {
        return isSensibleAttribute;
    }

    public double getInfoLossMultiplier() {
        return infoLossMultiplier;
    }

    @Override
    public String toString() {
        return "CastleRule{" +
                "position=" + position +
                ", generalizer=" + generalizer +
                ", isSensibleAttribute=" + isSensibleAttribute +
                ", informationLossMultiplier=" + infoLossMultiplier +
                '}';
    }
}
