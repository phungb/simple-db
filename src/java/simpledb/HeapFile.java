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
public class HeapFile implements DbFile, Serializable {

    private final File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
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
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile ran = new RandomAccessFile(f, "r");
            ran.seek(pid.getPageNumber() * BufferPool.getPageSize());
            byte[] buf = new byte[BufferPool.getPageSize()];
            ran.read(buf);
            return new HeapPage((HeapPageId) pid, buf);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            RandomAccessFile ran = new RandomAccessFile(f, "rw");
            ran.seek(page.getId().getPageNumber() * Database.getBufferPool().getPageSize());
            ran.write(page.getPageData());
            ran.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) f.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        for (int i = 0; i < this.numPages(); i++) {
            PageId pid = new HeapPageId(this.getId(), i);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (p.getNumEmptySlots() > 0) {
                p.insertTuple(t);
                p.markDirty(true, tid);

                return new ArrayList<Page>(Arrays.asList(p));
            }
        }

        HeapPageId pid = new HeapPageId(this.getId(), numPages());
        HeapPage p = new HeapPage(pid, HeapPage.createEmptyPageData());
        p.insertTuple(t);
        p.markDirty(true, tid);
        this.writePage(p);
        return new ArrayList<Page>(Arrays.asList(p));
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId rid = t.getRecordId();
        PageId pid = (HeapPageId) rid.getPageId();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        p.deleteTuple(t);
        p.markDirty(true, tid);

        return new ArrayList<Page>(Arrays.asList(p));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator, Serializable {
        private TransactionId tid;
        private int pageNum;
        private Iterator<Tuple> iter;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open() throws DbException, TransactionAbortedException {
            pageNum = 0;
            HeapPageId hpid = new HeapPageId(getId(), pageNum);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
            iter = page.iterator();
        }

        /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // iterator has not been opened or we reached end of file
            if (iter == null || pageNum >= numPages()) {
                return false;
            }

            // go next
            while (!iter.hasNext()) {
                pageNum++;

                if (pageNum >= numPages()) {
                    return false;
                }

                HeapPageId hpid = new HeapPageId(getId(), pageNum);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
                iter = page.iterator();
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
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements");
            } else {
                return iter.next();
            }
        }

        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        public void close() {
            pageNum = 0;
            iter = null;
        }
    }
}
