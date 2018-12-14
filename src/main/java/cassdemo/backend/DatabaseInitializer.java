package cassdemo.backend;

import cassdemo.Main;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import java.io.IOException;
import java.util.Properties;

public class DatabaseInitializer {
    private int blocks;
    private int blockSize;
    private String contactPoint;
    private String keyspace;
    private int replicationFactor;
    private Session session;

    public DatabaseInitializer(String propertiesFile){
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
    }

    public void init() throws BackendException {
        Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
        try {
            session = cluster.connect();//(keyspace);
        } catch (Exception e) {
            throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
        }
        try {
            createKeyspace();
            createTables();
            insertData();
        } catch (Exception e) {
            throw new BackendException("Operation failed. " + e.getMessage() + ".", e);
        }

    }

    private void insertData() {
        PreparedStatement psLock = session.prepare("INSERT INTO Lock(key, process) values (0, -1)");
        BoundStatement bsLock = psLock.bind();
        session.execute(bsLock);

        PreparedStatement psBlocks = session.prepare("INSERT INTO Blocks(block, process) VALUES (?, -1)");
        PreparedStatement psNumbers = session.prepare("INSERT INTO Numbers(block, number, process) VALUES (?, ?, -1)");

        for(int i =0; i<blocks; i++){
            BoundStatement bsBlocks = psBlocks.bind(i);
            session.execute(bsBlocks);

            for(int j =0; j<blockSize; j++){
                BoundStatement bsNumbers = psNumbers.bind(i, j);
                session.execute(bsNumbers);
            }
        }
    }

    private void createKeyspace() {
        PreparedStatement ps = session.prepare(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
        BoundStatement dropKeyspace = ps.bind();
        session.execute(dropKeyspace);

        ps = session.prepare(
                String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : %d};",
                        keyspace, replicationFactor));
        BoundStatement createKeyspace = ps.bind();
        session.execute(createKeyspace);

        ps = session.prepare(String.format("USE %s;", keyspace));
        BoundStatement useKeyspace = ps.bind();
        session.execute(useKeyspace);
    }

    private void createTables(){
        session.execute("CREATE TABLE Lock(\n" +
                "  key int PRIMARY KEY,\n" +
                "  process int,\n" +
                ");");

        session.execute("CREATE TABLE Blocks(\n" +
                "  block" +
                " int PRIMARY KEY,\n" +
                "  process int,\n" +
                ");");

        session.execute("CREATE TABLE Numbers(\n" +
                "  block int,\n" +
                "  number int,\n" +
                "  process int,\n" +
                "  PRIMARY KEY (block, number)" +
                ");");

    }

    protected void finalize() throws BackendException {
        try {
            if (session != null) {
                session.getCluster().close();
            }
        } catch (Exception e) {
            throw new BackendException("Could not close existing cluster " + e.getMessage() + ".", e);
        }
    }
    public static void main(String args[]){
        String filename = "config.properties";
        if (args.length >= 2)
            filename = args[1];
        DatabaseInitializer dbinit = new DatabaseInitializer(filename);
        try {
            dbinit.init();
        } catch (BackendException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
