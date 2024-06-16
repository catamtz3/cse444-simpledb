package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tuple;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tuple = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tuple;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws NoSuchElementException{
        try {
            // check if the table id matches the id. If not, throw exception
            if(pid.getTableId() != getId()) {
                throw new NoSuchElementException();
            }
            // if table id and id match, then seek to the page and read it. return the read page
            long offset = pid.getPageNumber() * Database.getBufferPool().getPageSize();
            RandomAccessFile randomFile = new RandomAccessFile(this.file, "r");
            randomFile.seek(offset);
            byte[] pgData = new byte[Database.getBufferPool().getPageSize()];
            randomFile.read(pgData);
            randomFile.close();
            HeapPage newPage = new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), pgData);
            return newPage;
        } catch (FileNotFoundException e) {
            e.printStackTrace(); 
            throw new NoSuchElementException("File not found: " + e.getMessage());
        } catch (IOException e) {
            e.printStackTrace(); 
            throw new NoSuchElementException("IO Exception occurred: " + e.getMessage());
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        long offset = page.getId().getPageNumber() * Database.getBufferPool().getPageSize();
        RandomAccessFile randomFile = new RandomAccessFile(this.file, "rw");
        randomFile.seek(offset);
        randomFile.write(page.getPageData());
        randomFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)Math.ceil(this.file.length()/Database.getBufferPool().getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // Go through the pages. If it has empty slots, insert the tuple onto the page and return the page
        ArrayList<Page> changedPage = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId heapPageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            if(page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                changedPage.add(page);
                return changedPage;
            }
        }
        // if no empty pages, create new page and append to physical file on disk 
        byte[] pgData = new byte[Database.getBufferPool().getPageSize()];
        HeapPageId heapPageId = new HeapPageId(getId(), numPages());
        HeapPage newPage = new HeapPage(heapPageId, pgData);
        newPage.insertTuple(t);
        this.writePage(newPage);
        changedPage.add(newPage);
        return changedPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // From the tuples page id, get the page. Remove the tuple from the page and return it
        ArrayList<Page> changedPage = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        changedPage.add(page);
        return changedPage;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        HeapFileIterator hfiter = new HeapFileIterator(tid, getId(), numPages());
        return hfiter;
    }
}

