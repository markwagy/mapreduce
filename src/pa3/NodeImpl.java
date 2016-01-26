package pa3;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 *
 * @author mark
 */
public interface NodeImpl extends Remote {
    DataPackage sortRows(DataPackage inDP, ArrayList<NetworkLocation> nodeList) 
            throws RemoteException;
    DataPackage sortRowsForPeer(DataPackage inDP) throws RemoteException;
    public ArrayList<DataRow> reduce(ArrayList<DataPackage> partitions) 
            throws RemoteException;
    public NodeStats getStats() throws RemoteException;
}
