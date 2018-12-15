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

    private static int RIGHTS_LOST = -2;
    private static int NOT_FOUND = -1;

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
        int[] triedBlocks = new int[blocks];
        int blocksLeft = blocks;
        if(count > blockSize){
            rr.erorMessage = "Request bigger than blocksize";
            return rr;
        }
        try {
            while(rr.result != ReservationResult.results.SUCCESS) {
                reserveLock(process);
                int reservedBlock = reserveBlock(process, triedBlocks);

                if(reservedBlock == NOT_FOUND){
                    rr.erorMessage = "No unlocked blocks";
                    return rr;
                }
                if(reservedBlock == RIGHTS_LOST)
                    continue;
                int reservedNumber;
                try {
                    reservedNumber = tryReserveNumbers(process, reservedBlock, count);
                    session.updateBlock(reservedBlock, -1);
                } catch (PartialSuccessException e) {
                    rr.result = ReservationResult.results.PARTIAL;
                    rr.erorMessage = e.getMessage();
                    rr.block = reservedBlock;
                    rr.number = e.start;
                    rr.count = e.fail - e.start +1;
                    return rr;
                }

                if(reservedNumber == NOT_FOUND){
                    triedBlocks[reservedBlock] = 1;
                    blocksLeft--;
                    if(blocksLeft == 0){
                        rr.erorMessage = "Not enough numbers";
                        return rr;
                    }
                    continue;
                }

                if(reservedNumber == RIGHTS_LOST)
                    continue;

                rr.result = ReservationResult.results.SUCCESS;
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

    public ReservationResult freeNumbers(int process, int block, int start, int count, boolean sure) throws PartialSuccessException {

        ReservationResult rr = new ReservationResult();
        ResultSet rs;
        try {
            rs = session.selectBlockNumbers(block);
        } catch (BackendException e) {
            rr.erorMessage = e.getMessage();
            return rr;
        }

        int[] numbers = new int[blockSize];
        for(Row row : rs){
            numbers[row.getInt("number")] = row.getInt("process");
        }
        int notOwnedNumbers = 0;
        for (int i = start; i < start + count && i < blockSize; i++) {

            if (numbers[i] == process) {
                try {
                    session.updateNumber(block, i, -1);
                } catch (BackendException e) {
                    rr.erorMessage = e.getMessage();
                    rr.result = ReservationResult.results.PARTIAL;
                    rr.block = block;
                    rr.number = start;
                    rr.count = i - start;
                    return rr;
                }
            }else{
                notOwnedNumbers++;
            }
        }

        if (sure && notOwnedNumbers > 0) {
            throw new PartialSuccessException(count, notOwnedNumbers, "Total of " + notOwnedNumbers + " from " + count + " to free were not owned");
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

        for(int i = start; i < (start + count); i++){
            try {
                session.updateNumber(reservedBlock, i, process);
            } catch (BackendException e) {
                throw new PartialSuccessException(start, i, e.getMessage());
            }
        }

        return start;
    }

    private int reserveBlock(int process, int[] triedBlocks) throws BackendException {
        int currentBlock = -1;
        while(true) {
            ResultSet rs = session.selectAllBlocks();
            for (Row row : rs) {
                int tmpProc = row.getInt("process");
                int tmpBlock = row.getInt("block");
                if (triedBlocks[tmpBlock]>0)continue;
                if (tmpProc < 0 || tmpProc == process) {
                    currentBlock = tmpBlock;
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

                        return currentBlock;
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
