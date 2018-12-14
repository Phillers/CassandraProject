package cassdemo;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import com.datastax.driver.core.Session;

import java.io.IOException;
import java.util.Properties;

public class Server {
    private int blocks;
    private int blockSize;
    private String contactPoint;
    private String keyspace;
    private int replicationFactor;

    private BackendSession session;

    public Server(String propertiesFile) throws BackendException {
        Properties properties = new Properties();
        try {
            properties.load(Main.class.getClassLoader().getResourceAsStream(propertiesFile));
            contactPoint = properties.getProperty("contact_point");
            blocks = Integer.parseInt(properties.getProperty("blocks"));
            blockSize = Integer.parseInt(properties.getProperty("blockSize"));
            keyspace = properties.getProperty("keyspace");
            replicationFactor = Integer.parseInt(properties.getProperty("replicationFactor"));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        session = new BackendSession(contactPoint, keyspace);
    }

    public ReservationResult reserveNumbers(int process, int count){
        ReservationResult rr = new ReservationResult();
        try {
            int currentLock;
            while ((currentLock = session.selectLock()) != process){
                if(currentLock < 0)
                    session.updateLock(process);
            }
        } catch (BackendException e){
            rr.success = false;
            rr.erorMessage = e.getMessage();
            return rr;
        }

        return rr;
    }
}
