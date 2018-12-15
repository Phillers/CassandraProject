package cassdemo;

import cassdemo.backend.BackendException;

import java.util.Random;
import java.util.Scanner;

public class Main {

    private static final String PROPERTIES_FILENAME = "config.properties";

    public static void main(String[] args) throws BackendException {
        Server server = new Server(PROPERTIES_FILENAME);
        Scanner scanner = new Scanner(System.in);
        ReservationResult rr;
        int process;
        if (args.length > 1)
            process = Integer.parseInt(args[1]);
        else
            process = new Random().nextInt(10);

        while (scanner.hasNext()) {
            char comand = scanner.next().charAt(0);
            if (comand == 'r') {

                rr = server.reserveNumbers(process, scanner.nextInt());
                System.out.println(String.format("Status: %s\n" +
                                "Block: %d\n" +
                                "Number: %d\n" +
                                "Count: %d\n" +
                                "Message: %s",
                        rr.result, rr.block, rr.number, rr.count, rr.erorMessage));
            }
            if (comand == 'f') {
                try {
                    server.freeNumbers(process, scanner.nextInt(), scanner.nextInt(), scanner.nextInt(), true);
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
