package json;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonObject {

	protected final JSONObject jsonObject;

	private boolean isDirty = false;
	private String stringValue = null;

	//

	public JsonObject() {
		super();
		this.jsonObject = new JSONObject();
	}

	public JsonObject(final String jsonString) {
		super();
		try {
			this.jsonObject = new JSONObject(jsonString);
		}
		catch (final Throwable e) {
			final String errorMessage = String.format("Cannot construct JsonObject with [%s]", jsonString);
			throw new JsonException(errorMessage, e);
		}
	}

	//TODO: Probably this is not required at all
	public JsonObject(final JSONObject jsonObj) {
		super();
		this.jsonObject = jsonObj;
	}

	//

	public String getString(final String key) {
		return this.jsonObject.optString(key, null);
	}

	public Boolean getBoolean(final String key) {
		Boolean returnValue = null;

		if (this.jsonObject.has(key)) {
			try {
				returnValue = this.jsonObject.getBoolean(key);
			}
			catch (final JSONException e) {
				throw new JsonException(e);
			}
		}

		return returnValue;
	}

	public Double getDouble(final String key) {
		Double returnValue = null;

		if (this.jsonObject.has(key)) {
			try {
				returnValue = this.jsonObject.getDouble(key);
			}
			catch (final JSONException e) {
				throw new JsonException(e);
			}
		}

		return returnValue;
	}

	public Integer getInt(final String key) {
		Integer returnValue = null;

		if (this.jsonObject.has(key)) {
			try {
				returnValue = this.jsonObject.getInt(key);
			}
			catch (final JSONException e) {
				throw new JsonException(e);
			}
		}

		return returnValue;
	}

	public Long getLong(final String key) {
		Long returnValue = null;

		if (this.jsonObject.has(key)) {
			try {
				returnValue = this.jsonObject.getLong(key);
			}
			catch (final JSONException e) {
				throw new JsonException(e);
			}
		}

		return returnValue;
	}

	public JsonArray getJsonArray(final String key) {
		JsonArray returnValue = null;

		if (this.jsonObject.has(key)) {
			try {
				final JSONArray jsonArray = this.jsonObject.getJSONArray(key);
				returnValue = new JsonArray(jsonArray);
			}
			catch (final JSONException e) {
				throw new JsonException(e);
			}
		}

		return returnValue;
	}

	public JsonObject getJsonObject(final String key) {
		JsonObject returnValue = null;

		if (this.jsonObject.has(key)) {
			try {
				final JSONObject jsonObj = this.jsonObject.getJSONObject(key);

				returnValue = new JsonObject(jsonObj); // TODO: Why we need to store the JSONObject, why not UserJSON Object?
			}
			catch (final JSONException e) {
				throw new JsonException(e);
			}
		}

		return returnValue;
	}

	//

	public boolean has(final String key) {
		return this.jsonObject.has(key);
	}

	//

	public JsonObject put(final String key, final Boolean value) {
		try {
			this.jsonObject.put(key, value);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public JsonObject put(final String key, final Double value) {
		try {
			this.jsonObject.put(key, value);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public JsonObject put(final String key, final Integer value) {
		try {
			this.jsonObject.put(key, value);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public JsonObject put(final String key, final Long value) {
		try {
			this.jsonObject.put(key, value);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public JsonObject put(final String key, final String value) {
		try {
			this.jsonObject.put(key, value);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public JsonObject put(final String key, final JsonObject value) {
		try {
			this.jsonObject.put(key, (value == null) ? null : value.jsonObject);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public JsonObject put(final String key, final JsonArray value) {
		try {
			this.jsonObject.put(key, (value == null) ? null : value.jsonArray);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public void remove(final String key) {
		this.jsonObject.remove(key);
		this.isDirty = true;
	}

	@Override
	public String toString() {

		if ((this.stringValue == null) || this.isDirty) {
			if (this.jsonObject != null) {
				this.stringValue = this.jsonObject.toString();
				this.isDirty = false;
			}
		}

		return this.stringValue;

	}

}
