package cassdemo;

import cassdemo.backend.BackendException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Client {

    private static final String PROPERTIES_FILENAME = "config.properties";

    public static void main(String[] args) throws BackendException {

        Server server = new Server(PROPERTIES_FILENAME);
        for (int i = 0; i < 80; i++) {
            final int param = 100 + i;
            Thread t = new Thread() {
                int tid = param;
                int maximumSleep = 50;
                int maximumNumbers = 10;
                List<ReservationResult> rrList = new ArrayList<>();
                List<ReservationResult> rrPartialList = new ArrayList<>();

                public void run() {
                    while (true) {
                        // Reserve blocks until the result is FAILED.
                        while (true) {
                            int numbersToReserve = ThreadLocalRandom.current().nextInt(1, maximumNumbers);
                            ReservationResult rr;
                            rr = server.reserveNumbers(tid, numbersToReserve);
                            System.out.println(String.format("Status: %s\t" +
                                            "Block: %d\t" + "Number: %d\t" + "Count: %d\t" + "Message: %s",
                                    rr.result, rr.block, rr.number, rr.count, rr.erorMessage));
                            if (rr.result == ReservationResult.results.SUCCESS) {
                                rrList.add(rr);
                            } else if (rr.result == ReservationResult.results.PARTIAL) {
                                rrPartialList.add(rr);
                            } else if (rr.result == ReservationResult.results.FAIL) {
                                break;
                            }
                            try {
                                int sleepTime = ThreadLocalRandom.current().nextInt(0, maximumSleep);
                                Thread.sleep(sleepTime);
                            } catch (InterruptedException v) {
                                System.out.println(v);
                            }
                        }
                        // Free all reserved blocks.
                        while (rrList.size() > 0) {
                            ReservationResult rr;
                            int randomItem = ThreadLocalRandom.current().nextInt(rrList.size());
                            rr = rrList.get(randomItem);
                            System.out.println("Freeing - tid = " + tid + " block = " + rr.block + " number = " + rr.number + " count = " + rr.count);
                            try {
                                ReservationResult rr2;
                                rr2 = server.freeNumbers(tid, rr.block, rr.number, rr.count, true);
                                System.out.println("Freeing status = " + rr2.result);
                            } catch (PartialSuccessException e) {
                                e.printStackTrace();
                            }
                            rrList.remove(randomItem);
                            try {
                                int sleepTime = ThreadLocalRandom.current().nextInt(0, maximumSleep);
                                Thread.sleep(sleepTime);
                            } catch (InterruptedException v) {
                                System.out.println(v);
                            }
                        }
                        while (rrPartialList.size() > 0) {
                            ReservationResult rr;
                            int randomItem = ThreadLocalRandom.current().nextInt(rrPartialList.size());
                            rr = rrPartialList.get(randomItem);
                            System.out.println("Freeing partial - tid = " + tid + " block = " + rr.block + " number = " + rr.number + " count = " + rr.count);
                            try {
                                server.freeNumbers(tid, rr.block, rr.number, rr.count, false);
                            } catch (PartialSuccessException e) {
                                e.printStackTrace();
                            }
                            rrPartialList.remove(randomItem);
                            try {
                                int sleepTime = ThreadLocalRandom.current().nextInt(0, maximumSleep);
                                Thread.sleep(sleepTime);
                            } catch (InterruptedException v) {
                                System.out.println(v);
                            }
                        }
                    }
                }
            };
            t.start();
        }
    }
}