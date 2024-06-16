package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    Predicate p;
    OpIterator child;
    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    /**
    * Opens the childs iterator
    */
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        super.open();
    }

    /**
    * Closes the childs iterator
    */
    public void close() {
        super.close();
        child.close();
    }

    /**
    * Resets the childs iterator
    */
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // Iterate through the childs values and compare to the operand field.
        // If the filter returns true, then return the value. Else, return null
        while (child.hasNext()) {
            Tuple t = child.next();
            if(p.filter(t)) {
                return t;
            }
        }
        return null;
    }

    /**
     * Returns an OpIterator[] filled with the values of the current child
     */
    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    /**
     * Sets the child to the values passed in from the parameter
     * array children
     */
    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
