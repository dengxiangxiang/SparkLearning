package json;

public class JsonException extends RuntimeException {

	private static final long serialVersionUID = -2729733031154121455L;

	public JsonException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public JsonException(final String message) {
		super(message);
	}

	public JsonException(final Throwable cause) {
		super(cause);
	}
}
