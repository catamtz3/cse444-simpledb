package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId t;
    private OpIterator child;
    private boolean wasDeleted; 

    public Delete(TransactionId t, OpIterator child) {
        this.t = t;
        this.child = child;
        this.wasDeleted = false;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // Check if the current tuple was deleted already. If it was, return null.
        // If not, go on to remove the tuple while there are some in the child. 
        // Then, return a tuple with the number of records we deleted.
        if(!wasDeleted) {
            int numRemoved = 0;
            while(child.hasNext()) {
                Tuple tuple = child.next();
                try {
                    Database.getBufferPool().deleteTuple(this.t, tuple);
                    numRemoved++;
                } catch (IOException f) {
                    f.printStackTrace();
                }
            }
        
            Tuple returnTuple = new Tuple(this.getTupleDesc());
            returnTuple.setField(0, new IntField(numRemoved));
            this.wasDeleted = true;
            return returnTuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
