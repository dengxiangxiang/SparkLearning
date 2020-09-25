package common.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Session {
    @JsonSetter("credentials_type")
    private String credentialsType;
    @JsonSetter("user_id")
    private String userId;
    @JsonSetter("application_id")
    private String applicationId;
    @JsonSetter("device_info")
    private DeviceInfo deviceInfo;
    @JsonSetter("credentials_key")
    private String credentialsKey;

    public String getCredentialsKey() {
        return credentialsKey;
    }

    public void setCredentialsKey(String credentialsKey) {
        this.credentialsKey = credentialsKey;
    }

    public String getCredentialsType() {
        return credentialsType;
    }

    public void setCredentialsType(String credentialsType) {
        this.credentialsType = credentialsType;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public DeviceInfo getDeviceInfo() {
        return deviceInfo;
    }

    public void setDeviceInfo(DeviceInfo deviceInfo) {
        this.deviceInfo = deviceInfo;
    }
}
