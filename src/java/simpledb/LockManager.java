package simpledb;

import java.io.*;
import java.util.*;

public class LockManager {

    private HashMap<TransactionId, HashSet<TransactionId>> dependencies;
    private HashMap<TransactionId, HashSet<PageId>> transactions;
    private HashMap<PageId, HashSet<TransactionId>> shared;
    private HashMap<PageId, TransactionId> exclusive;

    public LockManager() {
        dependencies = new HashMap<TransactionId, HashSet<TransactionId>>();
        transactions = new HashMap<TransactionId, HashSet<PageId>>();
        shared = new HashMap<PageId, HashSet<TransactionId>>();
        exclusive = new HashMap<PageId, TransactionId>();
    }

    /**
     * Returns true if lock is granted for a specific tid and pid with permissions perm
     */
    public synchronized boolean grantLock(TransactionId tid, PageId pid, Permissions perm) {
      boolean aq;
      if (perm.equals(Permissions.READ_ONLY)) {
          aq = acquireShared(tid, pid);
          //System.out.println("SHARED REQUEST   : TID: " + tid + " PID: " + pid + " GRANTED?: " + aq);

      } else {
          aq = acquireExclusive(tid, pid);
          //System.out.println("EXCLUSIVE REQUEST: TID: " + tid + " PID: " + pid + " GRANTED?: " + aq);
      }
      //System.out.println("DEPENDENCIES: " + getDependencies());
      return aq;
    }

    /**
     * Returns true if a shared lock is granted for a specific tid and pid
     */
    public synchronized boolean acquireShared(TransactionId tid, PageId pid) {
        // if like is held return true
        if (holdsLock(tid, pid)) {
              return true;
          }

        // if exclusive lock is held and not from this tid, return false
        if (exclusive.containsKey(pid)) {
            // add dependency from tid to tid that holds exclusive lock
            HashSet<TransactionId> neighbors = dependencies.get(tid);
            if (neighbors == null) {
                neighbors = new HashSet<TransactionId>();
            }
            neighbors.add(exclusive.get(pid));
            dependencies.put(tid, neighbors);

            return false;
        }

        // Lock can be granted
        // remove from dependency graph and grant lock
        dependencies.remove(tid);
        HashSet<TransactionId> tids = shared.get(pid);
        if (tids == null) {
            tids = new HashSet<TransactionId>();
        }

        HashSet<PageId> pids = transactions.get(tid);
        if (pids == null) {
            pids = new HashSet<PageId>();
        }
        pids.add(pid);
        transactions.put(tid, pids);
        tids.add(tid);
        shared.put(pid, tids);
        return true;
    }

    /**
     * Returns true if a exclusive lock is granted for a specific tid and pid
     */
    public synchronized boolean acquireExclusive(TransactionId tid, PageId pid) {
        // if like is held return true
        if (holdsExclusive(tid, pid)) {
            return true;
        }

        // if exclusive lock is held and not from this tid, return false
        if (exclusive.containsKey(pid)) {
            // add dependency from tid to tid that holds exclusive lock
            HashSet<TransactionId> neighbors = dependencies.get(tid);
            if (neighbors == null) {
                neighbors = new HashSet<TransactionId>();
            }
            neighbors.add(exclusive.get(pid));
            dependencies.put(tid, neighbors);

            return false;
        }

        // has shared lock but not exclusive lock
        // further examine situation
        if (shared.containsKey(pid)) {

            // if only one shared lock has been granted and this shared lock is for
            // this tid upgrade it to exclusive
            HashSet<TransactionId> tids = shared.get(pid);
            if ((tids.contains(tid) && tids.size() == 1) || tids.size() == 0) {
                shared.remove(pid);
            } else {
                // we are waiting on all other transactions that has a shared lock
                HashSet<TransactionId> neighbors = dependencies.get(tid);
                if (neighbors == null) {
                    neighbors = new HashSet<TransactionId>();
                }
                neighbors.addAll(tids);
                dependencies.put(tid, neighbors);
                dependencies.get(tid).remove(tid);

                return false;
            }
        }

        // grant exclusive lock and remove from dependency graph
        dependencies.remove(tid);
        HashSet<PageId> pids = transactions.get(tid);
        if (pids == null) {
            pids = new HashSet<PageId>();
        }
        pids.add(pid);
        transactions.put(tid, pids);

        exclusive.put(pid, tid);
        return true;
    }

    /**
      * Detects Deadlock. A cycle within the dependencies graph is found
      */
    public synchronized boolean detectDeadlock(TransactionId tid) {
        // use Dfs to detect cycle from dependency graph
        //System.out.println("detecting Deadlock");
        HashSet<TransactionId> explored = new HashSet<TransactionId>();
        Stack<TransactionId> s = new Stack<TransactionId>();
        s.push(tid);
        while (!s.isEmpty()) {
            TransactionId curr = s.pop();
            explored.add(curr);
            HashSet<TransactionId> next = dependencies.get(curr);
            if (next == null) {
                continue;
            }
            if (next.contains(tid)) {
                return true;
            } else {
                for (TransactionId t : next) {
                    if (explored.contains(t)) {
                        return true;
                    }
                    s.push(t);
                }
            }
        }
        return false;
    }

    /**
     * Remove tid from dependency graph
     */
    public void removeDependency(TransactionId tid) {
        dependencies.remove(tid);
        for (TransactionId t: dependencies.keySet()) {
            HashSet<TransactionId> tids = dependencies.get(t);
            tids.remove(tid);
            dependencies.put(t, tids);
        }
    }

    /**
     * Releases the lock on a transaction on the specified page
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        HashSet<TransactionId> tids = shared.get(pid);
        if (tids != null && tids.contains(tid)) {
            tids.remove(tid);
        }
        exclusive.remove(pid);
        removeDependency(tid);
    }

    /**
     * Releases all locks associated with the give transactionid
     */
    public synchronized void releaseLock(TransactionId tid) {
        if (transactions.get(tid) == null) return;
        for (PageId pid : transactions.get(tid)) {
            HashSet<TransactionId> tids = shared.get(pid);
            if (tids != null && tids.contains(tid)) {
                tids.remove(tid);
            }
            exclusive.remove(pid);
        }
        transactions.remove(tid);
        removeDependency(tid);
    }

    /** Releases all locks */
    public void releaseAll() {
        for (PageId p : shared.keySet()) {
            shared.remove(p);
        }

        for (PageId p : exclusive.keySet()) {
            exclusive.remove(p);
        }
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return holdsExclusive(tid, pid) || holdsShared(tid, pid);
    }

    public boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        if (perm == Permissions.READ_ONLY) {
            return holdsShared(tid, pid);
        } else {
            return holdsExclusive(tid, pid);
        }
    }

    /**
     * Return true if there is a exclusive lock on a transaction and page
     */
    public boolean holdsExclusive(TransactionId tid, PageId pid) {
        if (exclusive.containsKey(pid) && exclusive.get(pid).equals(tid)) {
            return true;
        }
        return false;
    }

    /**
     * Return true if there is a shared lock on a transaction and page
     */
    public boolean holdsShared(TransactionId tid, PageId pid) {
        if (shared.containsKey(pid)) {
            HashSet<TransactionId> tids = shared.get(pid);
            if (tids.contains(tid)) {
                return true;
            }
        }
        return false;
    }

    // for debugging purposes
    public String getDependencies() {
        return dependencies.toString();
    }
}
