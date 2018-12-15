package cassdemo;

public class ReservationResult {
    public enum results {
         FAIL, SUCCESS, PARTIAL
    }

    public results result = results.FAIL;
    public int block;
    public int number;
    public int count;
    public String erorMessage = "";
}
