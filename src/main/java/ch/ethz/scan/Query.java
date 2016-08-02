package ch.ethz.scan;

import ch.ethz.kv.CassandraKv;
import ch.ethz.kv.HBaseKv;
import ch.ethz.kv.Kv;
import ch.ethz.kv.MongoKv;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static ch.ethz.scan.QueryBroker.*;

/**
 * Created by renatomarroquin on 2016-08-01.
 */
public class Query {
    public Map<String, Scan> scans;

    public Query() {
        scans = new HashMap<>();
    }

    public void addScan(Scan sc) {
        this.scans.put(sc.getKv().getType(), sc);
    }

    public static Query doQuery(int qId) {
        Query q = new Query();
        Random r = new Random();
        switch (qId) {
            case 1:
                q.addScan(new Scan<>("lineitem", "l_shipdate", r.nextLong(), RangeOp.LOWER_EQ, new HBaseKv()));
                break;
            case 2:
                q.addScan(new Scan<>("nation", "n_regionkey", r.nextInt(), RangeOp.LOWER, new MongoKv()));
                q.addScan(new Scan<>("region", "r_name", "Name", RangeOp.LIKE, new MongoKv()));
                q.addScan(new Scan<>("part", "p_type", "Type", RangeOp.LIKE, new MongoKv()));
                q.addScan(new Scan<>("partsup", "ps_partkey", r.nextLong(), RangeOp.GREATER, new HBaseKv()));
                q.addScan(new Scan<>("supplier", "ps_suppkey", r.nextLong(), RangeOp.GREATER, new CassandraKv()));
                break;
        }
        return q;
    }

    public static class Scan<T> {
        private String table;
        private String col;
        private T value;
        private RangeOp op;
        private Kv kv;

        public Scan(String t, String c, T v, RangeOp o, Kv kvs) {
            this.setCol(c);
            this.setTable(t);
            this.setValue(v);
            this.setOp(o);
            this.setKv(kvs);
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getCol() {
            return col;
        }

        public void setCol(String col) {
            this.col = col;
        }

        public T getValue() {
            return value;
        }

        public void setValue(T value) {
            this.value = value;
        }

        public RangeOp getOp() {
            return op;
        }

        public void setOp(RangeOp op) {
            this.op = op;
        }

        public Kv getKv() {
            return kv;
        }

        public void setKv(Kv kv) {
            this.kv = kv;
        }
    }

}
