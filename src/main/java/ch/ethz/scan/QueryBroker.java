package ch.ethz.scan;

/**
 * Created by renatomarroquin on 2015-11-09.
 */
public class QueryBroker {

    public enum RangeOp {
        GREATER(">"), GREATER_EQ(">="), LOWER("<"), LOWER_EQ("<="), LIKE("%");
        private String val;
        RangeOp(String v) {
            this.val = v;
        }
    }

    static public void main(String[] args) {
        // one table per kvs
    }
}
