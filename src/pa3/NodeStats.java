package pa3;

import java.io.Serializable;

/**
 * NodeStats
 * class to contain information about a node to display statistics
 * @author mark
 */
public class NodeStats implements Serializable {
    
    /**
     * Load statistics
     */
    public double currentLoad;
    public double averageLoad;
    
    /**
     * The number of times this node has been asked to do a sort task
     */
    public int numberOfTasksSeen;
    
    /*
     * the number of times this node has had to request help 
     * from its peers due to excess system load
     */
    public int numberOfMigrations; 
    
    /**
     * number of times this node has gone down
     */
    public int numberOfFaults;
    
    /**
     * Node location that these stats are associated with
     */
    NetworkLocation location;
    
    /*
     * default constructor
     */
    public NodeStats(NetworkLocation location) { 
        currentLoad = 0.0;
        averageLoad = 0.0;
        numberOfTasksSeen = 0;
        numberOfMigrations = 0;
        this.location = location;
    }
    
    public String toString() {
        return  "  :: Node Stats :: \n" + 
                ". current load         = " + currentLoad + "\n" + 
                ". average load         = " + averageLoad + "\n" + 
                ". number of tasks seen = " + numberOfTasksSeen + "\n" + 
                ". number of migrations = " + numberOfMigrations + "\n\n";
    }
}
