package common.entity;


import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class ProcessResult {
    private Long logTime;
    private String userId;
    private String applicationId;
    private String credentialType;
    private String credentialKey;
    private String deviceId;
    private String requestName;
    private String statusCode;

//    public ProcessResult(
//            Long time,
//            String userId,
//            String applicationId,
//            String credentialType,
//            String credentialKey,
//            String deviceId) {
//        this(time,userId,applicationId,credentialType,credentialKey,deviceId,RequestUrl.Other);
//    }

    public ProcessResult(Long time,
                         String userId,
                         String applicationId,
                         String credentialType,
                         String credentialKey,
                         String deviceId,
                         String requestName,
                         String statusCode) {
        this.logTime = time;
        this.userId = userId;
        this.applicationId = applicationId;
        this.credentialType = credentialType;
        this.credentialKey = credentialKey;
        this.deviceId = deviceId;
        this.requestName = requestName;
        this.statusCode = statusCode;

    }

    public String toString() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        format.setTimeZone(TimeZone.getTimeZone(UserStatsTaskUtils.timeZone));
        return String.format(
                "time:%s,userId:%s,applicationId:%s,credentialType:%s,credentialKey:%s,deviceId:%s",
                format.format(logTime), userId, applicationId, credentialType, credentialKey, deviceId);
    }

    public String getUserId() {
        return userId;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getCredentialType() {
        return credentialType;
    }

    public String getCredentialKey() {
        return credentialKey;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public Long getLogTime() {
        return logTime;
    }

    public String getRequestName() {
        return requestName;
    }

    public String getStatusCode() {
        return statusCode;
    }


}
