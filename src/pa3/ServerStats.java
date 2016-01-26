package pa3;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * ServerStats
 * Class to house a number of server statistics as per reqs
 * @author mark
 */
public class ServerStats implements Serializable {
    
    public int numberOfFaults;
    public double averageLoad;
    public int numberOfSortRequestsSinceStartup;
    
    /**
     * maintain a list of all of the node's statistics
     */
    ArrayList<NodeStats> nodeStatList;
    
    /**
     * Default constructor
     */
    public ServerStats () {
        nodeStatList = new ArrayList<NodeStats>();
        numberOfFaults = 0;
        averageLoad = 0.0;
        numberOfSortRequestsSinceStartup = 0;
    }
    
    /**
     * Total number of migrations from node to node as a result of load
     * in the entire system.
     */
    private int getTotalNumberOfJobMigrations() {
        int total = 0;
        for (NodeStats n : nodeStatList) {
            total += n.numberOfMigrations;
        }
        return total;
    }
    
    private double getTotalLoad() {
        double total = 0.0;
        for (NodeStats n : nodeStatList) {
            total += n.currentLoad;
        }            
        return total;
    }
    
    private double getAverageLoad() {
        return getTotalLoad() / nodeStatList.size();
    }
    
    private int getNumberOfFaults() {
        int num = 0;
        for (NodeStats n : nodeStatList) {
            num += n.numberOfFaults;
        }
        return num;
    }
    
    private int getNumberOfRequestsSinceStartup() {
        return this.numberOfSortRequestsSinceStartup;
    }
    
    public void setNodeStats(ArrayList<NodeStats> ns) {
        this.nodeStatList = ns;
    }
    
    @Override
    public String toString() {
        return    " :: Server Stats ::\n" 
                + ". total number of requests since startup = " 
                + getNumberOfRequestsSinceStartup() + "\n"
                + ". total number of faults (all nodes) =-- = "
                + getNumberOfFaults() + "\n"
                + ". average load over all nodes ---------- = "
                + getAverageLoad() + "\n"
                + ". total number of job migrations ------- = "
                + getTotalNumberOfJobMigrations() + "\n";
    }
}
