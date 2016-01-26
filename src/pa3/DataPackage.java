package pa3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;


/**
 * DataPackage class
 * This class is the abstraction for the "data work package" that is passed from
 * Server to Node and vice versa.
 * 
 * @author mark
 */
public class DataPackage implements Serializable {
    
    private ArrayList<DataRow> row;
    
    private boolean sorted;
    
    /**
     * Constructor for DataPackage
     */
    public DataPackage() { 
        row = new ArrayList<DataRow>();
        sorted = false;
    }
    
    /*
     * Constructor from existing rows
     */
    public DataPackage(ArrayList<DataRow> rows) {
        row = new ArrayList<DataRow>(rows);
        sorted = false;
    }
    
    /**
     * Constructor from existing array of rows
     */
    public DataPackage(DataRow[] r) {
        this(DataPackage.drArrayToList(r));
    }
    
    /**
     * Get data package from file
     */
    public void addFromFile(String fileName) {
        try {
            BufferedReader reader =
                    new BufferedReader(new FileReader(fileName));
            if(reader == null){
                 System.out.println("File reader is null");
            }
            String line = null;
            while ((line = reader.readLine()) != null) {
                row.add(new DataRow(line));
            }
        } catch (IOException e) {
            System.err.println("ERROR: Unable to parse words file: " + e);
        }    
    }

    /**
     * Write data package to file
     */
    public void writeToFile(String fileName) {
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(fileName));
            for (DataRow r : row) {
                writer.write(r.getStringVal() + "\n");
            }
            writer.close();
        } catch (IOException ex) {
            System.err.print("Error writing DataPackage: " + ex);
        }
    }
    
    /**
     * Getter for size
     */
    public int getSize() {
        return row.size();
    }
    
    /**
     * Getter for all rows in this data package
     */
    public ArrayList<DataRow> getRows() {
        return row;
    }
    
    /**
     * append another data package on to this one
     */
    public void append(DataPackage dp) {
        ArrayList<DataRow> rows = dp.getRows();
        this.row.addAll(rows);
    }
    
        /**
     * Convert from DataRow array to a list
     */
    public static ArrayList<DataRow> drArrayToList(DataRow[] arr) {
        ArrayList<DataRow> li = new ArrayList<DataRow>();
        for (DataRow d : arr) {
            li.add(d);
        }
        return li;
    }
    
    /**
     * Returns whether this data package has been sorted
     */
    public boolean isSorted() {
        return sorted;
    }
    
    /**
     * Setter for "sorted", indicating whether this DP is sorted or not
     */
    public void sorted(boolean isSorted) {
        sorted = isSorted;
    }
    
}
