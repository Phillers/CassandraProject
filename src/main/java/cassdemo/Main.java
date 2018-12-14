package cassdemo;

import cassdemo.backend.BackendException;

public class Main {

	private static final String PROPERTIES_FILENAME = "config.properties";

	public static void main(String[] args) throws BackendException {
		Server server = new Server(PROPERTIES_FILENAME);


		System.exit(0);
	}
}
