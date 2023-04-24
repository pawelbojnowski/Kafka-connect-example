package pl.pb.kafkaconnectexample.config;

public class Commons {

	private Commons() {
	}

	public static void println(final String format, final Object... args) {
		System.out.println(String.format(format, args));
	}

	public static void println() {
		System.out.println();
	}


}
