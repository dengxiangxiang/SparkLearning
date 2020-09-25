package common.json;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonArray {

	JSONArray jsonArray;

	private boolean isDirty = false;
	private String stringValue = null;

	//

	public JsonArray() {
		super();
		this.jsonArray = new JSONArray();
	}

	public JsonArray(final String jsonArrayString) {
		super();
		try {
			this.jsonArray = new JSONArray(jsonArrayString);
		}
		catch (final Throwable e) {
			final String errorMessage = String.format("Cannot construct JsonArray with [%s]", jsonArrayString);
			throw new JsonException(errorMessage, e);
		}
	}

	protected JsonArray(final JSONArray jsonArray) {
		super();
		this.jsonArray = jsonArray;
	}

	//

	public JsonArray put(final Boolean value) {
		this.jsonArray.put(value);
		this.isDirty = true;
		return this;
	}

	public JsonArray put(final Double index) {
		this.jsonArray.put(index);
		this.isDirty = true;
		return this;
	}

	public JsonArray put(final Integer value) {
		this.jsonArray.put(value);
		this.isDirty = true;
		return this;
	}

	public JsonArray put(final Long value) {
		this.jsonArray.put(value);
		this.isDirty = true;
		return this;
	}

	public JsonArray put(final String value) {
		this.jsonArray.put(value);
		this.isDirty = true;
		return this;
	}

	public JsonArray put(final JsonObject value) {
		this.jsonArray.put((value == null) ? null : value.jsonObject);
		this.isDirty = true;
		return this;
	}

	public JsonArray put(final JsonArray value) {
		this.jsonArray.put((value == null) ? null : value.jsonArray);
		this.isDirty = true;
		return this;
	}

	public JsonArray put(final int index, final Boolean value) {
		try {
			this.jsonArray.put(index, value);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}

	}

	public JsonArray put(final int index, final Double value) {
		try {
			this.jsonArray.put(index, value);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public JsonArray put(final int index, final Integer value) {
		try {
			this.jsonArray.put(index, value);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public JsonArray put(final int index, final Long value) {
		try {
			this.jsonArray.put(index, value);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public JsonArray put(final int index, final String value) {
		try {
			this.jsonArray.put(index, value);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public JsonArray put(final int index, final JsonObject value) {
		try {
			this.jsonArray.put(index, (value == null) ? null : value.jsonObject);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}

	}

	public JsonArray put(final int index, final JsonArray value) {
		try {
			this.jsonArray.put(index, (value == null) ? null : value.jsonArray);
			this.isDirty = true;
			return this;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	//

	public Boolean getBoolean(final int index) {
		try {
			return this.jsonArray.getBoolean(index);
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public Double getDouble(final int index) {
		try {
			return this.jsonArray.getDouble(index);
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public Integer getInt(final int index) {
		try {
			return this.jsonArray.getInt(index);
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public JsonArray getJsonArray(final int index) {
		try {
			final JSONArray arrays = this.jsonArray.getJSONArray(index);

			final JsonArray returnValue = new JsonArray();
			returnValue.jsonArray = arrays;

			return returnValue;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public JsonObject getJsonObject(final int index) {

		try {
			final JSONObject obj = this.jsonArray.getJSONObject(index);

			final JsonObject returnValue = new JsonObject(obj);

			return returnValue;
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public Long getLong(final int index) {
		try {
			return this.jsonArray.getLong(index);
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public String getString(final int index) {
		try {
			return this.jsonArray.getString(index);
		}
		catch (final JSONException e) {
			throw new JsonException(e);
		}
	}

	public boolean isNull(final int index) {
		return this.jsonArray.isNull(index);
	}

	public int length() {
		return this.jsonArray.length();
	}

	@Override
	public String toString() {
		if ((this.stringValue == null) || this.isDirty) {
			if (this.jsonArray != null) {
				this.stringValue = this.jsonArray.toString();
				this.isDirty = false;
			}
		}

		return this.stringValue;
	}

}
