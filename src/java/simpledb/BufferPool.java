package simpledb;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
 
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private ConcurrentHashMap<PageId, Page> buffer;
    private int numPages;
    private LockManager lockManager;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        buffer = new ConcurrentHashMap<>();
        this.numPages = numPages;
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // Aquire the desired lock type before retrieving page information
        try {
            if(perm == Permissions.READ_ONLY){
                lockManager.acquire(tid, pid,LockType.SHARED);
            } else {
                lockManager.acquire(tid, pid,LockType.EXCLUSIVE);
            }
            // if the bufferpool size is bigger than the number of pages, evict pages until 
            // it's not
            while (buffer.size() >= numPages){
                evictPage();
            } 
            // Check if the bufferpool has the desired page. If it does, return the page
            if (buffer.containsKey(pid)){
                return buffer.get(pid);
            // If it doesn't have the page, then read the page and put it into bufferpool
            } else {
                int tableId = pid.getTableId();
                Catalog current = Database.getCatalog();
                DbFile temp = current.getDatabaseFile(tableId);
                Page page = temp.readPage(pid);
                buffer.put(pid, page);
                return page;
            }
        } catch (InterruptedException e){
            Thread.currentThread().interrupt(); // Good practice to re-interrupt the thread
            throw new TransactionAbortedException(); // Convert to a transaction-specific exception
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        lockManager.release(tid, pid);
    }

    public int getNumPages(){
        return this.numPages;
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.getLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // If the transaction is commited, flush pages to disk and release all locks
        if(commit){
            Set<PageId> pages = new HashSet<>(lockManager.getTPages(tid));
            for(PageId page: pages){
                Page p = this.buffer.get(page);
                Database.getLogFile().logWrite(tid, p.getBeforeImage(), p);
                Database.getLogFile().force();
                p.setBeforeImage();
                lockManager.release(tid, page);
            }
        // If the transaction is aborted, retrieve the transactions page information and replace it
        // in the buffer pool. Then, release all locks associated with the transaction
        } else {
            Set<PageId> pages = new HashSet<>(lockManager.getTPages(tid));
            for(PageId page: pages){
                int tableId = page.getTableId();
                Catalog current = Database.getCatalog();
                DbFile temp = current.getDatabaseFile(tableId);
                Page apage = temp.readPage(page);
                buffer.put(page, apage);
                lockManager.release(tid, page);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t) 
        throws DbException, IOException, TransactionAbortedException {
        HeapFile heapfile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pagesMod = heapfile.insertTuple(tid, t);
            for (Page page : pagesMod) {
                page.markDirty(true, tid);
                this.buffer.put(page.getId(), page);
            }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableid = t.getRecordId().getPageId().getTableId();
        HeapFile heapfile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        ArrayList<Page> pagesMod = heapfile.deleteTuple(tid, t);
        for (Page page : pagesMod) {
            page.markDirty(true, tid);
            buffer.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // Go through all pages in buffer pool.
        // Check if page is dirty,if it is, then write the page and unmark it as dirty.
        for(PageId pid : this.buffer.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        this.buffer.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // Check if the bufferpool has the pid, if it does, then get the page. 
        // Check if page is dirty,if it is, then write the page and unmark it as dirty.
        if(this.buffer.containsKey(pid)){
            Page pg = this.buffer.get(pid);
            if(pg.isDirty() != null) {
                HeapFile heapfile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
                // append an update record to the log, with
                // a before-image and after-image.
                TransactionId dirtier = pg.isDirty();
                if (dirtier != null && lockManager.getLock(dirtier, pid)){
                    Database.getLogFile().logWrite(dirtier, pg.getBeforeImage(), pg);
                    Database.getLogFile().force();
                }
                heapfile.writePage(pg);
                pg.markDirty(false, null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // Retrieve all pages associated with a given transaction. Go through each page
        // and if it's in the bufferpool, retrieve it. If it's dirty, write it to disk.
        // Unmark the written page as dirty.
        Set<PageId> pages = new HashSet<>(lockManager.getTPages(tid));
        for(PageId page: pages){
            Page pg = this.buffer.get(page);
            if (tid.equals(pg.isDirty())) {
                flushPage(pg.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // Get Pages from the bufferpool. Add them to a list if they're not dirty and don't have a lock.
        ArrayList<PageId> pids = new ArrayList<>();
        for (PageId pid : buffer.keySet()) {
            pids.add(pid);
        }
        // If there's no potential pages to evict, throw a DBException.
        if (pids.isEmpty()) {
            throw new DbException("No pages to evict.");
        }
        // If there's pages to evict, pick a random one, flush it, and discard it.
        PageId toEvict = pids.get(ThreadLocalRandom.current().nextInt(pids.size()));
        try {
            flushPage(toEvict); 
        } catch (IOException e) {
            throw new DbException("Failed to flush page to disk: " + e.getMessage());
        }
        discardPage(toEvict);  
    }
}
