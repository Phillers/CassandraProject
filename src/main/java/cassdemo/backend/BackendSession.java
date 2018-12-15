package cassdemo.backend;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * For error handling done right see: 
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 * 
 * Performing stress tests often results in numerous WriteTimeoutExceptions, 
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and 
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	public static BackendSession instance = null;

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint)
				.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
				.build();
		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}

	private static PreparedStatement SELECT_ALL_FROM_BLOCKS;
	private static PreparedStatement SELECT_FROM_BLOCKS;
	private static PreparedStatement SELECT_FROM_NUMBERS;
	private static PreparedStatement SELECT_FROM_LOCK;
	private static PreparedStatement UPDATE_LOCK;
	private static PreparedStatement UPDATE_BLOCKS;
	private static PreparedStatement UPDATE_NUMBERS;

	private void prepareStatements() throws BackendException {
		try {
			SELECT_ALL_FROM_BLOCKS = session.prepare("SELECT * FROM Blocks;");
			SELECT_FROM_BLOCKS = session.prepare("SELECT * FROM Blocks WHERE block=?;");
			SELECT_FROM_LOCK = session.prepare("SELECT * FROM Lock WHERE key=0;");
			SELECT_FROM_NUMBERS = session.prepare("SELECT * FROM Numbers WHERE block=?;");
			UPDATE_BLOCKS = session.prepare(
					"UPDATE Blocks set process=? WHERE block=?;");
			UPDATE_LOCK = session.prepare(
					"UPDATE Lock set process=? WHERE key=0;");
			UPDATE_NUMBERS = session.prepare(
					"UPDATE Numbers set process=? WHERE block=? AND number=?;");

		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public ResultSet selectAllBlocks() throws BackendException {

		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_BLOCKS);

		ResultSet rs;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs;
	}

	public int selectBlock(int block) throws BackendException {

		BoundStatement bs = SELECT_FROM_BLOCKS.bind(block);

		ResultSet rs;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		return rs.one().getInt("process");
	}

	public int selectLock() throws BackendException {

		BoundStatement bs = new BoundStatement(SELECT_FROM_LOCK);

		ResultSet rs;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		return rs.one().getInt("process");

	}

	public ResultSet selectBlockNumbers(int block) throws BackendException {

		BoundStatement bs = SELECT_FROM_NUMBERS.bind(block);

		ResultSet rs;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs;
	}

	public void updateLock(int process) throws BackendException {
		BoundStatement bs = UPDATE_LOCK.bind(process);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an update. " + e.getMessage() + ".", e);
		}

		logger.info(String.format("Lock reserved by process %d", process));
	}

	public void updateBlock(int block, int process) throws BackendException {
		BoundStatement bs = UPDATE_BLOCKS.bind(process, block);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an update. " + e.getMessage() + ".", e);
		}

		logger.info(String.format("Block %d reserved by process %d", block, process));
	}
	public void updateNumber(int block, int number, int process) throws BackendException {
		BoundStatement bs = UPDATE_NUMBERS.bind(process, block, number);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an update. " + e.getMessage() + ".", e);
		}

		logger.info(String.format("Number %d in block %d reserved by process %d", number, block, process));
	}

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}
