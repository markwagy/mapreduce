package pa3;

import java.io.Serializable;

/**
 * DataRow class
 * Basic Class to house a row of data to be included in DataPackage
 * @implements Comparable so that we can sort it
 * @author mark
 */
public class DataRow implements Comparable <DataRow>, Serializable {

    private String stringVal;
    private Integer intVal;
    
    /**
     * Constructor for DataRow
     * @param line 
     */
    DataRow(String line) {
        stringVal = line.trim();
        intVal = new Integer(stringVal);
    }

    @Override
    public int compareTo(DataRow t) {
//        return this.stringVal.compareToIgnoreCase(t.getStringVal());
        return intVal.compareTo(t.getIntVal());
    }
    
    public String getStringVal() {
        return stringVal;
    }
    
    public Integer getIntVal() {
        return intVal;
    }
    
    /**
     * Generic "getter" for whatever type the value ends up being
     * @return 
     */
    public Integer get() {
///        return stringVal;
        return intVal;
    }
    
}
