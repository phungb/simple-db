package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field, Integer> counts;
    private Map<Field, Integer> aggr;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this. afield = afield;
        this.what = what;

        aggr = new HashMap<Field, Integer>();
        counts = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        // result is a pair of from (groupValue, aggregateValue)
        // if value of group by field was Aggregator.NO_GROUPING it takes form of (aggregateValue)

        Field gv;

        if (gbfield != NO_GROUPING) {
            gv = tup.getField(gbfield);
        } else {
            gv = null;
        }

        Integer nextV = ((IntField) tup.getField(afield)).getValue();
        Integer oldV = aggr.get(gv);

        // initialize
        if (!aggr.containsKey(gv)) {
            aggr.put(gv, nextV);
            counts.put(gv, 1);
        } else {
            switch (what) {
                case AVG:
                case SUM:
                case COUNT:
                    aggr.put(gv, oldV + nextV);
                    break;

                case MIN:
                    aggr.put(gv, Math.min(oldV, nextV));
                    break;
                case MAX:
                    aggr.put(gv, Math.max(oldV, nextV));
                    break;
            }
            counts.put(gv, counts.get(gv) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc td;
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();

        // result is a pair of from (groupValue, aggregateValue)
        // if value of group by field was Aggregator.NO_GROUPING it takes form of (aggregateValue)
        if (gbfield == NO_GROUPING) {
          td = new TupleDesc(new Type[] {Type.INT_TYPE});
          Tuple t = new Tuple(td);

          int value = Calculate(aggr.get(null), counts.get(null));

          t.setField(0, new IntField(value));

          tuples.add(t);

        } else {
            td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
            for (Field f : aggr.keySet()) {
                Tuple t = new Tuple(td);

                int value = Calculate(aggr.get(f), counts.get(f));

                t.setField(0, f);
                t.setField(1, new IntField(value));

                tuples.add(t);
            }

        }


        return new TupleIterator(td, tuples);
    }

    private int Calculate(int value, int count) {
        switch (what) {
          case MIN:
          case MAX:
          case SUM:
              return value;
          case COUNT:
              return count;
          case AVG:

              return value / count;

          }
          return 0;
    }

}
