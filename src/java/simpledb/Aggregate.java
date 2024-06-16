package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    OpIterator child;
    int afield;
    int gfield;
    Aggregator.Op aop;
    Aggregator agg;
    OpIterator aggItr;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        // depending on the type of the column over which we are grouping the result, initialize 
        // either an IntegerAggregator or a StringAggregator
        Type gfieldType;
        if(this.gfield == -1){
            gfieldType = null;
        } else {
            gfieldType = child.getTupleDesc().getFieldType(gfield);
        }
    
        if (child.getTupleDesc().getFieldType(this.afield).equals(Type.INT_TYPE)) {
            this.agg = new IntegerAggregator(this.gfield, gfieldType, afield, aop);
        } else {
            this.agg = new StringAggregator(this.gfield, gfieldType, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        return child.getTupleDesc().getFieldName(this.gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    /**
     * opens child and class's iterators, and add all of the tuples from 
     * the OpIterator into the Aggregator
     * */
    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        super.open();
        child.open();

        while (child.hasNext()) {
            agg.mergeTupleIntoGroup(child.next());
        }

        this.aggItr = agg.iterator();
        aggItr.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if (aggItr != null && aggItr.hasNext()) {
            return aggItr.next();
        }

        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        aggItr.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        Type[] typearr; 
        String[] fieldarr;

        if(gfield != -1) {
            typearr = new Type[2];
            fieldarr = new String[2];
            typearr[0] = child.getTupleDesc().getFieldType(this.gfield);
            fieldarr[0] = child.getTupleDesc().getFieldName(this.gfield);
            typearr[1] = child.getTupleDesc().getFieldType(this.afield);
            fieldarr[1] = nameOfAggregatorOp(aop) + " (" + child.getTupleDesc().getFieldName(this.afield) + ")";
        } else {
            typearr = new Type[1];
            fieldarr = new String[1];
            typearr[0] = child.getTupleDesc().getFieldType(this.afield);
            fieldarr[0] = nameOfAggregatorOp(aop) + " (" + child.getTupleDesc().getFieldName(this.afield) + ")";
        }

        return new TupleDesc(typearr, fieldarr);
    }

    /**
     * closes child and class's iterators, as well as the aggregator's iterator 
     * */
    public void close() {
        super.close();
        if(aggItr != null) {
            aggItr.close();
            aggItr = null;
        }
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
	    return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
    
}
