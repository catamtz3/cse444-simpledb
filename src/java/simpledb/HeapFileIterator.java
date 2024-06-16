package simpledb;
import java.io.*;
import java.util.*;

/**
 * DbFileIterator is the iterator interface that all SimpleDB Dbfile should
 * implement.
 */
public class HeapFileIterator implements DbFileIterator{
    private int currPage;
    Iterator<Tuple> i = null;
    TransactionId tid;
    int tableid;
    int numPages;
    
    /**
     * Constructs an iterator from the specified Iterable, and the specified
     * descriptor.
     * 
     * @param tuples
     *            The set of tuples to iterate over
     */
    public HeapFileIterator(TransactionId tid, int tableid, int numPages) {
        this.tid = tid;
        this.tableid = tableid;
        this.numPages = numPages;
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open() throws DbException, TransactionAbortedException {
        this.currPage = 0;
        this.i = setPage();
    }
    
    public Iterator<Tuple> setPage() throws DbException, TransactionAbortedException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid, new HeapPageId(this.tableid , this.currPage), Permissions.READ_ONLY);
        return page.iterator();
    }

    /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(this.i == null || this.currPage >= this.numPages) {
            return false;  
        }
        while(!this.i.hasNext()) { 
            this.currPage++;
            if(this.currPage >= this.numPages) {
                return false;
            } 
            this.i = setPage();
        } 
        return true;
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if(hasNext()) {
            return this.i.next();
        } else {
            throw new NoSuchElementException();
        }
    }
        
    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
        i = null;
    }
}
