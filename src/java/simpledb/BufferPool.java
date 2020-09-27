package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    //private ArrayList<Page> pool;
    //private final HashMap<PageId, Integer> pageIdToIndex;
    private HashMap<PageId, Page> pageIdToPage;
    private int numPages;

    private HashMap<PageId, Integer> pidToOrder;
    private TreeMap<Integer, PageId> orderToPid;
    private int timestamp = 0;

    private LockManager lm;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        // pool = new ArrayList<Page>();
        // pageIdToIndex = new HashMap<PageId, Integer>();
        pageIdToPage = new HashMap<PageId, Page>();
        this.numPages = numPages;

        pidToOrder = new HashMap<PageId, Integer>();
        orderToPid = new TreeMap<Integer, PageId>();
        lm = new LockManager();
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        boolean lockGranted = lm.grantLock(tid, pid, perm);
        if (!lockGranted) {
            boolean waited = false;
            lockGranted = lm.grantLock(tid, pid, perm);
            while (!lockGranted) {
                //System.out.println("DEPENDENCIES: " + lm.getDependencies());
                // Deadlock dtected so we release all locks this tid has acquired and abort
                if (lm.detectDeadlock(tid)) {
                    //System.out.println("DEADLOCK DETECTED");
                    lm.releaseLock(tid);
                    throw new TransactionAbortedException();
                }

                // try to acquire lock in a few moments
                try {
                    Thread.sleep(10);
                    lockGranted = lm.grantLock(tid, pid, perm);
                } catch (Exception e) {
                    System.out.println("exception");
                }
            }
        }


        if (pageIdToPage.containsKey(pid)) {
            cachePage(pageIdToPage.get(pid));
            return pageIdToPage.get(pid);
        }

        Page retPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        cachePage(retPage);
        return retPage;
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
        // some code goes here
        // not necessary for lab1|lab2
        lm.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lm.holdsLock(tid, p);
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
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            flushPages(tid);
            for (PageId pid : pageIdToPage.keySet()) {
                TransactionId id = pageIdToPage.get(pid).isDirty();
                //if (id != null && id.equals(tid)) {
                    Page p = pageIdToPage.get(pid);
                    p.setBeforeImage();
                //}
            }
        } else {
            HashSet<PageId> discard = new HashSet<>();
            for (PageId pid : pageIdToPage.keySet()) {
                TransactionId id = pageIdToPage.get(pid).isDirty();
                if (id != null && id.equals(tid)) {
                    discard.add(pid);
                    //discardPage(pid);
                }
            }
            for (PageId d : discard) {
                discardPage(d);
            }
        }
        lm.releaseLock(tid);
        //System.out.println("Transaction complete: " + lm.getDependencies());
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
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtied = f.insertTuple(tid, t);
        for (Page p : dirtied) {
            p.markDirty(true, tid);
            cachePage(p);
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
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        ArrayList<Page> deleted = f.deleteTuple(tid, t);

        for (Page p : deleted) {
            p.markDirty(true, tid);
            cachePage(p);

        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid : pageIdToPage.keySet()) {
            if (pageIdToPage.get(pid).isDirty() != null) {
                flushPage(pid);
            }
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
        // some code goes here
        // not necessary for lab1
        pageIdToPage.remove(pid);
        Integer n = pidToOrder.remove(pid);
        if (n == null) {
           return;
        }
        orderToPid.remove(n);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page p = pageIdToPage.get(pid);

        TransactionId dirtier = p.isDirty();
        if (dirtier != null){
          Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
          Database.getLogFile().force();
        }


        if (p == null) {
            return;
        }

        int tableId = pid.getTableId();
        Database.getCatalog().getDatabaseFile(tableId).writePage(p);
        p.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : pageIdToPage.keySet()) {
            TransactionId id = pageIdToPage.get(pid).isDirty();
            if (id != null && id.equals(tid)) {
                flushPage(pid);
                releasePage(tid, pid);
            }
        }
        lm.releaseLock(tid);
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // try {
            // for (Integer i : orderToPid.keySet()) {
                PageId last = orderToPid.firstEntry().getValue();
                // if (pageIdToPage.get(last) == null) {
                //     continue;
                // }
              //  if (pageIdToPage.get(last).isDirty() == null) {
                    pidToOrder.remove(last);
                    orderToPid.remove(orderToPid.firstKey());
                    pageIdToPage.remove(last);
                    try {
                        flushPage(last);
                    } catch (Exception E) {}
                    return;
              //  }
            // }
            // throw new DbException("ALL PAGES ARE DIRTY");
        // } catch (Exception e) {
            // e.printStackTrace();
        // }
    }

    /**
      * Caches Page p into BufferPool
      * Evicts page if BufferPool is full
      * Evicts page that was used last
      */
    private void cachePage(Page p) throws DbException {
        timestamp++;
        HeapPageId pid = (HeapPageId) p.getId();

        // page not full make sure it is in the BufferPool
        if (pageIdToPage.size() < numPages || pageIdToPage.containsKey(pid)) {

            // BufferPool already has page so update timestamp
            if (pageIdToPage.containsKey(pid)) {
                orderToPid.put(timestamp, pid);

                int time = pidToOrder.get(pid); // time previously used;
                orderToPid.remove(time);

                pidToOrder.put(pid, timestamp);
            } else {
            // BufferPool does not have page
            // have to create new entry

                pageIdToPage.put(pid, p);
                orderToPid.put(timestamp, pid);
                pidToOrder.put(pid, timestamp);

            }
        } else { // BufferPool is full
                 // Evict a page
                evictPage();

                // add page
                pidToOrder.put(pid, timestamp);
                orderToPid.put(timestamp, pid);
                pageIdToPage.put(pid, p);

        }


    }

}
