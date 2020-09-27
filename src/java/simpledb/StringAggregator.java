package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field, Integer> counts;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this. afield = afield;
        this.what = what;

        counts = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gv;

        if (gbfield != NO_GROUPING) {
            gv = tup.getField(gbfield);
        } else {
            gv = null;
        }

        if (counts.containsKey(gv)) {
            counts.put(gv, counts.get(gv) + 1);
        } else {
            counts.put(gv, 1);
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // some code goes here
        TupleDesc td;
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();

        // result is a pair of from (groupValue, aggregateValue)
        // if value of group by field was Aggregator.NO_GROUPING it takes form of (aggregateValue)
        if (gbfield == NO_GROUPING) {
          td = new TupleDesc(new Type[] {Type.INT_TYPE});


          if (counts.isEmpty()) {
              Tuple t = new Tuple(td);
              t.setField(0, new IntField(counts.get(null)));
              tuples.add(t);
          }




        } else {
            td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
            for (Field f : counts.keySet()) {
                Tuple t = new Tuple(td);

                t.setField(0, f);
                t.setField(1, new IntField(counts.get(f)));

                tuples.add(t);
            }

        }


        return new TupleIterator(td, tuples);
    }

}
