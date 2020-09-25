package common.entity;

public class AggregateResult {

    private int yearMonth;
    private String applicationId;
    private String userId;
    private String credentialType;
    private String credentialKey;
    private String deviceId;
    private int requestNum;

    public int getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(int yearMonth) {
        this.yearMonth = yearMonth;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getCredentialType() {
        return credentialType;
    }

    public void setCredentialType(String credentialType) {
        this.credentialType = credentialType;
    }

    public String getCredentialKey() {
        return credentialKey;
    }

    public void setCredentialKey(String credentialKey) {
        this.credentialKey = credentialKey;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public int getRequestNum() {
        return requestNum;
    }

    public void setRequestNum(int requestNum) {
        this.requestNum = requestNum;
    }

    public AggregateResult(int yearMonth, String applicationId, String userId, String credentialType, String credentialKey, String deviceId, int requestNum){
        this.yearMonth = yearMonth;
        this.applicationId = applicationId;
        this.userId = userId;
        this.credentialType = credentialType;
        this.credentialKey = credentialKey;
        this.deviceId = deviceId;
        this.requestNum = requestNum;
    }


}
