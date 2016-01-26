package pa3;

import java.io.Serializable;

/**
 * Class to house the idea of a protocol
 * to be passed to remote objects and determine their behavior
 * @author mark
 */
public class Protocol implements Serializable {
    
    /**
     * Protocol types
     */
    public enum Type {SORTDATA, NODEJOIN, REDUCE, STATS, SORTFORPEER};    
    private Type type;

    /**
     * Constructor
     */
    public Protocol(Type t) {
        this.type = t;
    }

    /**
     * getter for protocol type
     */
    public Type getType() {
        return type;
    }
    
    /**
     * Override of toString method
     */
    @Override
    public String toString() {
        if (type == Type.NODEJOIN) {
            return "{NODEJOIN}";
        } else if (type == Type.SORTDATA) {
            return "{SORTDATA}";
        } else if (type == Type.REDUCE) {
            return "{REDUCE}";
        } else if (type == Type.STATS) {
            return "{STATS}";
        } else if (type == Type.SORTFORPEER) {
            return "{SORTFORPEER}";
        }
        return "{UNKNOWN}";
    }
}
