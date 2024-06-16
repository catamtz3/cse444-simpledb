package simpledb;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, Integer> grouping;
    private ConcurrentHashMap<Field, Integer> groupingCount;

    /**
     * Initializes a new IntegerAggregator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        grouping = new ConcurrentHashMap<>();
        groupingCount = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // Get the current field. If there's no grouping, set the current field is null
        Field current;
        if (gbfield == Aggregator.NO_GROUPING){
            current = new IntField(-1);
        } else {
            current = tup.getField(gbfield);
        }
        // Calculate the aggregate value based on the field and afields value
        int value = ((IntField) tup.getField(afield)).getValue();
        
        // Check what operation is requested. Check if the operation is grouped or not, and based
        // on this, calculate the correct aggregated value for the operation and store it
        if(what == Op.MAX){
            if(!grouping.containsKey(current)) {
                grouping.put(current, value);
            } else {
                grouping.put(current, Math.max(value, grouping.get(current)));
            }
        } 
        if(what == Op.SUM){
            if(!grouping.containsKey(current)) {
                grouping.put(current, value);
            } else {
                grouping.put(current, value + grouping.get(current));
            }
        } 
        if(what == Op.COUNT){
            if(!grouping.containsKey(current)) {
                grouping.put(current, 1);
            } else {
                grouping.put(current, grouping.get(current)+1);
            }
        } 
        if(what == Op.AVG){
            if(!grouping.containsKey(current)) {
                grouping.put(current, (value));
                groupingCount.put(current, 1);
            } else {
                grouping.put(current, (value + grouping.get(current)));
                groupingCount.put(current, groupingCount.get(current) + 1);
            }
        } 
        if(what == Op.MIN){
            if(!grouping.containsKey(current)) {
                grouping.put(current, value);
            } else {
                grouping.put(current, Math.min(value, grouping.get(current)));
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
        for (Map.Entry<Field, Integer> entry : grouping.entrySet()) {
            int aggVal = entry.getValue();
            Tuple newTuple = new Tuple(new TupleDesc(typearr));
            if(what == Op.AVG ){
                int count = groupingCount.get(entry.getKey());
                aggVal = aggVal/count;
            }
            if(gbfield == NO_GROUPING) {
                newTuple.setField(0, new IntField(aggVal));
            } else {
                newTuple.setField(0, (entry.getKey()));
                newTuple.setField(1, new IntField(aggVal));
            }
            tuples.add(newTuple);
        }
        return new TupleIterator(new TupleDesc(typearr), tuples);
    }

}
