package simpledb.parallel;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.session.IoSession;

import simpledb.*;
import simpledb.OpIterator;

/**
 * The producer part of the Shuffle Exchange operator.
 *
 * ShuffleProducer distributes tuples to the workers according to some
 * partition function (provided as a PartitionFunction object during the
 * ShuffleProducer's instantiation).
 *
 * */
public class ShuffleProducer extends Producer {

    private static final long serialVersionUID = 1L;

    private transient WorkingThread runningThread;

    private OpIterator child;
    private ParallelOperatorID operatorID;
    private SocketInfo[] workers;
    private PartitionFunction<?, ?> pf;

    public String getName() {
        return "shuffle_p";
    }

    public ShuffleProducer(OpIterator child, ParallelOperatorID operatorID,
                           SocketInfo[] workers, PartitionFunction<?, ?> pf) {
        super(operatorID);
        // some code goes here
        this.child = child;
        this.operatorID = operatorID;
        this.workers = workers;
        this.pf = pf;
    }

    public void setPartitionFunction(PartitionFunction<?, ?> pf) {
        // some code goes here
        this.pf = pf;
    }

    public SocketInfo[] getWorkers() {
        // some code goes here
        return workers;
    }

    public PartitionFunction<?, ?> getPartitionFunction() {
        // some code goes here
        return pf;
    }

    // some code goes here
    class WorkingThread extends Thread {
        public void run() {
            // some code goes here

            IoSession[] sessions = new IoSession[ShuffleProducer.this.workers.length];
            ArrayList<ArrayList<Tuple>> buffers = new ArrayList<ArrayList<Tuple>>();
            for (int i = 0; i < ShuffleProducer.this.workers.length; i++) {
                sessions[i] = ParallelUtility.createSession(
                        ShuffleProducer.this.workers[i].getAddress(),
                        ShuffleProducer.this.getThisWorker().minaHandler, -1);
                buffers.add(new ArrayList<Tuple>());
            }

            try {
                long lastTime = System.currentTimeMillis();

                while (ShuffleProducer.this.child.hasNext()) {
                    Tuple tup = ShuffleProducer.this.child.next();
                    int partition = pf.partition(tup, ShuffleProducer.this.child.getTupleDesc());

                    ArrayList<Tuple> buffer = buffers.get(partition);
                    IoSession session = sessions[partition];

                    buffer.add(tup);
                    int cnt = buffer.size();
                    if (cnt >= TupleBag.MAX_SIZE) {
                        session.write(new TupleBag(
                                ShuffleProducer.this.operatorID,
                                ShuffleProducer.this.getThisWorker().workerID,
                                buffer.toArray(new Tuple[] {}),
                                ShuffleProducer.this.getTupleDesc()));
                        buffer.clear();
                        lastTime = System.currentTimeMillis();
                    }
                    if (cnt >= TupleBag.MIN_SIZE) {
                        long thisTime = System.currentTimeMillis();
                        if (thisTime - lastTime > TupleBag.MAX_MS) {
                            session.write(new TupleBag(
                                    ShuffleProducer.this.operatorID,
                                    ShuffleProducer.this.getThisWorker().workerID,
                                    buffer.toArray(new Tuple[] {}),
                                    ShuffleProducer.this.getTupleDesc()));
                            buffer.clear();
                            lastTime = thisTime;
                        }
                    }
                }

                for (int i = 0; i < buffers.size(); i++) {
                    ArrayList<Tuple> buffer = buffers.get(i);
                    IoSession session = sessions[i];

                    if (buffer.size() > 0)
                        session.write(new TupleBag(ShuffleProducer.this.operatorID,
                                ShuffleProducer.this.getThisWorker().workerID,
                                buffer.toArray(new Tuple[] {}),
                                ShuffleProducer.this.getTupleDesc()));
                    session.write(new TupleBag(ShuffleProducer.this.operatorID,
                            ShuffleProducer.this.getThisWorker().workerID)).addListener(new IoFutureListener<WriteFuture>(){

                                @Override
                                public void operationComplete(WriteFuture future) {
                                    ParallelUtility.closeSession(future.getSession());
                                }});//.awaitUninterruptibly(); //wait until all the data have successfully transfered
                }
                } catch (DbException e) {
                    e.printStackTrace();
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                }
            }
        }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        this.runningThread = new WorkingThread();
        this.runningThread.start();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.child.getTupleDesc();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            // wait until the working thread terminate and return an empty tuple set
            runningThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
