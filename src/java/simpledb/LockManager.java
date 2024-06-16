package simpledb;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/*
 * The Lock Manager class is responsible for acquiring and releasing locks for transactions. 
 * It also assigns keeps tracks of the locks per page, the set of pages for each transaction id, and 
 * performs cycle detection to detect deadlock. 
 */
public class LockManager {

    // Lock Class. Stores the type of lock (shared/exclusive) and the associated transactions
    private static class Lock {
        Set<TransactionId> sharedLock;
        TransactionId transaction;

        // Initialize an instance of Lock
        public Lock(){
            transaction = null;
            sharedLock = new HashSet<>();
        }

        // Acquire the desired lock
        public synchronized boolean acquire(TransactionId tid, LockType type){
            // If LockType is shared and there's not already an exlusive lock in place
            // or the current transaction equals tid, add the transaction to shared lock 
            if(type == LockType.SHARED){
                if(transaction == null || transaction.equals(tid)){
                    sharedLock.add(tid);
                    return true;
                } else {
                    return false;
                } 
            // If LockType is exclusive and there's not already a lock in place, acquire the lock.
            // Also handles lock upgrading if there's only the current transaction in the shared lock.
            } else if (type == LockType.EXCLUSIVE){
                if((transaction == null || transaction.equals(tid)) && (sharedLock.isEmpty() || (sharedLock.size() == 1 && sharedLock.contains(tid)))){
                    transaction = tid;
                    sharedLock.clear();
                    return true;   
                } else {
                    return false;
                }
            }
            return false;
        }

        // Releases the desired transactions lock
        public synchronized boolean release(TransactionId tid){
            // Remove the transaction from the appropriate lock type
            if(tid != null){
                if(transaction != null){
                    if(transaction.equals(tid)){
                        transaction = null;
                        return true;
                    }
                } else if (sharedLock.contains(tid)){
                    sharedLock.remove(tid);
                    return true;
                } else {
                    return false;
                }
            }
            return false;
        }

        // Returns if a transaction has a lock or not
        public boolean hasLock(TransactionId tid){
            return sharedLock.contains(tid) || (transaction != null && transaction.equals(tid));
        }

    }
    
    // Maps to keep track of locks, the pages a transaction has touched, and cycledetection for a given transaction
    private Map<PageId, Lock> locks;
    private Map<TransactionId, Set<PageId>> tpages;  
    private Map<TransactionId, Set<TransactionId>> getCycle;

    // Initialize an instance of LockManager
    public LockManager(){
        locks = new ConcurrentHashMap<>();
        tpages = new ConcurrentHashMap<>();
        getCycle = new ConcurrentHashMap<>();
    }

    // Returns true if a tid has a specific page in its lock
    public boolean getLock(TransactionId tid, PageId page){
        return locks.containsKey(page) && locks.get(page).hasLock(tid);
    }

    // Returns true if a page has a lock or not
    public boolean getLock(PageId page){
        return locks.containsKey(page);
    }

    // Acquires the desired lock for a tid and page. Uses Cycle Detection and timeouts to handle deadlock.
    public boolean acquire(TransactionId tid, PageId page, LockType type) throws InterruptedException, TransactionAbortedException {
        synchronized (this) {
            // Retrieve or create the lock if it doesn't exist. While the current transaction can't acquire a lock, 
            // wait. Method times out after 30000 ms. 
            Lock lock = locks.computeIfAbsent(page, newLock -> new Lock());
            int count = 0;
            while ((lock.transaction != null && !lock.transaction.equals(tid)) || 
                (type == LockType.EXCLUSIVE && !lock.sharedLock.isEmpty() && !(lock.sharedLock.size() == 1 && lock.sharedLock.contains(tid)))) {
                wait(10000);
                count++;
                if(count > 2){
                    throw new TransactionAbortedException();
                }
            }

            // Acquire lock. Check for cycles, throw an error immediately if a cycle is detected
            boolean lockAcquired = lock.acquire(tid, type);
            if (lockAcquired) {
                if (lock.transaction == null) { // if shared lock, add several dependencies
                    for (TransactionId currT : lock.sharedLock) {
                        addDependency(tid, currT);
                    }
                } else { // else add one dependency
                    addDependency(tid, lock.transaction);
                }
            }
            if (checkCycle(tid)) {
                removeDependency(tid);
                throw new TransactionAbortedException();
            }
            
            // Add page to the set of pages connected to a given transaction, then notify all that lock is acquired
            if (lockAcquired && tpages.containsKey(tid)) {
                tpages.get(tid).add(page);
            } else if (lockAcquired) {
                Set<PageId> curr = new HashSet<>();
                curr.add(page);
                tpages.put(tid, curr);
            }
            notifyAll();
            return true;
        }
    }

    // Private method: Add dependency to getCycle map
    private void addDependency(TransactionId tid, TransactionId depTid) {
        if (!tid.equals(depTid)) { 
            getCycle.computeIfAbsent(tid, newDep -> new HashSet<>()).add(depTid);
        }
    }
    // Private method: Remove dependency to getCycle map
    private void removeDependency(TransactionId tid) {
        getCycle.remove(tid);
        for (Set<TransactionId> deps : getCycle.values()) {
            deps.remove(tid);
        }
    }

    // Private method: Checks if there's a cycle for a given transaction in the getCycle method
    private boolean checkCycle(TransactionId tid){
        Set<TransactionId> seen = new HashSet<>();
        return hasCycle(tid, seen);
    }

    // Recursively checks if the getCycle map has a cycle for a given transaction. 
    // Returns true if it does, else false
    private boolean hasCycle(TransactionId tid, Set<TransactionId> seen) {
        if (seen.contains(tid)) {
            return true;
        }
        seen.add(tid);
        Set<TransactionId> currDep = getCycle.get(tid);
        if (currDep != null) {
            for (TransactionId dep : currDep) {
                if (hasCycle(dep, seen)) {
                    seen.remove(tid); 
                    return true;
                }
            }
        }
        seen.remove(tid);  
        return false;
    }

    // Returns the set of pages a given transaction has touched
    public synchronized Set<PageId> getTPages(TransactionId tid){
        return tpages.get(tid);
    }

    // Releases a lock for a given tid and page. Removes its dependencies and information from the maps,
    // then notifies all that the lock has been released
    public synchronized void release(TransactionId tid, PageId page){
        Lock lock = locks.get(page);
        if(lock != null && lock.release(tid)){
            removeDependency(tid);
            if (lock.transaction == null && lock.sharedLock.isEmpty()) {
                locks.remove(page);
                Set<PageId> curr = tpages.get(tid);
                curr.remove(page);
                if(!curr.isEmpty()){
                    tpages.put(tid,curr);
                }
            }
        }
        notifyAll();
    }

}
// Declare LockType
enum LockType {
    SHARED,
    EXCLUSIVE
}