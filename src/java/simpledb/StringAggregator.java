package simpledb;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    private ConcurrentHashMap<Field, Integer> aggregatorMap;
    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("supports only COUNT operation");
        }
        this.aggregatorMap = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // Get the current field. If there's no grouping, set the current field is null.
        // Calculate the aggregated value and store it
        Field groupByField;
        if (gbfield == NO_GROUPING) {
            groupByField = null;
        } else {
            groupByField = tup.getField(gbfield);
        }
        int aggregateVal = aggregatorMap.getOrDefault(groupByField, 0);
        aggregatorMap.put(groupByField, aggregateVal + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // Create a list of tuples. If the gbfield typle is no grouping, the tuples are of field INT_TYPE.
        // If the tuples are grouped, they're of type gbfieldtype and Type.INT_TYPE.
        List<Tuple> tuples = new ArrayList<>();
        Type[] typearr;
        if(gbfield == NO_GROUPING) {
            typearr = new Type[]{Type.INT_TYPE};
        } else {
            typearr = new Type[]{gbfieldtype, Type.INT_TYPE};
        }
        // Iterate through the entries of the grouped tuples. Set the fields
        // correctly based on if they're no grouping or grouped. Add the modified tuples
        // to our list of tuples and return them as a TupleIterator
        for (Map.Entry<Field, Integer> entry : aggregatorMap.entrySet()) {

            Tuple newTuple = new Tuple(new TupleDesc(typearr));
            if(gbfield == NO_GROUPING) {
                newTuple.setField(0, new IntField(entry.getValue()));
            } else {
                newTuple.setField(0, (entry.getKey()));
                newTuple.setField(1, new IntField(entry.getValue()));
            }
            tuples.add(newTuple);
        }
        return new TupleIterator(new TupleDesc(typearr), tuples);
    }

}
