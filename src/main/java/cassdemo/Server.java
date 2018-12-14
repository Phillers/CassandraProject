package cassdemo;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

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
        if(count > blockSize){
            rr.erorMessage = "Request bigger than blocksize";
            return rr;
        }
        try {
            while(!rr.success) {
                reserveLock(process);
                int reservedBlock = reserveBlock(process);
                if(reservedBlock == -1){
                    rr.erorMessage = "No unlocked blocks";
                    return rr;
                }
                if(reservedBlock == -2)
                    continue;
                int reservedNumber = -2;
                try {
                    reservedNumber = tryReserveNumbers(process, reservedBlock, count);
                } catch (PartialSuccessException e) {
                    rr.erorMessage = e.getMessage();
                    rr.block = reservedBlock;
                    rr.number = e.start;
                    rr.count = e.fail = e.start;
                }

                if(reservedNumber == -1){
                    rr.erorMessage = "Not enough numbers in reserved block";
                    return rr;
                }
                if(reservedNumber == -2)
                    continue;

                rr.success = true;
                rr.count = count;
                rr.number = reservedNumber;
                rr.block = reservedBlock;
                return rr;


            }
        } catch (BackendException e){
            rr.erorMessage = e.getMessage();
            return rr;
        }

        return rr;
    }

    private int tryReserveNumbers(int process, int reservedBlock, int count) throws PartialSuccessException, BackendException {
        ResultSet rs = session.selectBlockNumbers(reservedBlock);
        int start = -1;
        int free = 0;
        int[] numbers = new int[blockSize];
        for(Row row : rs){
            numbers[row.getInt("number")] = row.getInt("process");
        }
        for(int i =0 ; i<blockSize; i++){
            if(numbers[i] == -1){
                if(start < 0)start = i;
                free +=1;
                if(free == count)
                    break;
            }else{
                start = -1;
                free = 0;
            }
        }

        if(free < count)
            return -1;

        if(session.selectBlock(reservedBlock) != process)
            return -2;

        for(int i = start; i < (start + free); i++){
            try {
                session.updateNumber(reservedBlock, start + i, process);
            } catch (BackendException e) {
                throw new PartialSuccessException(start, i, e.getMessage());
            }
        }

        return start;
    }

    private int reserveBlock(int process) throws BackendException {
        int currentBlock = -1;
        int lockedBlock = -1;
        while(true) {
            ResultSet rs = session.selectAllBlocks();
            for (Row row : rs) {
                int tmpProc = row.getInt("process");
                if (tmpProc < 0 || tmpProc == process) {
                    currentBlock = row.getInt("block");
                    break;
                }
            }

            if (currentBlock < 0) {
                return -1;
            }

            if (session.selectLock() != process)
                return -2;

            session.updateBlock(currentBlock, process);

            rs = session.selectAllBlocks();
            for (Row row : rs) {
                if (row.getInt("block") == currentBlock) {
                    if(row.getInt("process") == process){
                        session.updateLock(-1);
                        lockedBlock = currentBlock;
                        return lockedBlock;
                    }
                    break;
                }
            }
        }
    }

    private void reserveLock(int process) throws BackendException {
        while (session.selectLock() != process) {
            if (session.selectLock() < 0)
                session.updateLock(process);
        }
    }


}
