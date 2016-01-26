package pa3;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 *
 * @author mark
 */
public interface ServerImpl extends Remote {
    void addNodeInfo(NetworkLocation nodeNetworkLocation) throws RemoteException;
    DataPackage acceptDataPackageForSort(DataPackage dataPackage)
            throws RemoteException;
}
