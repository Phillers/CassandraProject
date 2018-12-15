package cassdemo;

import cassdemo.backend.BackendException;

import java.util.Scanner;

public class Main {

    private static final String PROPERTIES_FILENAME = "config.properties";

    public static void main(String[] args) throws BackendException {
        Server server = new Server(PROPERTIES_FILENAME);
        Scanner scanner = new Scanner(System.in);
        ReservationResult rr;
        while (scanner.hasNext()) {
            char comand = scanner.next().charAt(0);
            if (comand == 'r') {

                rr = server.reserveNumbers(0, scanner.nextInt());
                System.out.println(String.format("Status: %s\n" +
                                "Block: %d\n" +
                                "Number: %d\n" +
                                "Count: %d\n" +
                                "Message: %s",
                        rr.result, rr.block, rr.number, rr.count, rr.erorMessage));
            }
            if (comand == 'f') {
                try {
                    server.freeNumbers(0, scanner.nextInt(), scanner.nextInt(), scanner.nextInt(), true);
                } catch (PartialSuccessException e) {
                    e.printStackTrace();
                }
            }
            if (comand == 'e')
                break;
        }


        System.exit(0);
    }
}
