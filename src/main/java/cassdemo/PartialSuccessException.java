package cassdemo;

class PartialSuccessException extends Exception {
    int start;
    int fail;
    PartialSuccessException(int start, int fail, String message){
        super(message);
        this.start = start;
        this.fail  = fail;
    }
}
