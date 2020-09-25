package common;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.entity.*;
import common.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LogProcessor {
    private static Logger logger = LoggerFactory.getLogger(LogProcessor.class);
    private static final String STR_MESSAGE = "message";
    private static final String STR_SESSION = "session";
    private static final String STR_REQUEST = "request";
    private static final String STR_RESPONSE = "response";
    private static final String STR_URI = "uri";
    private static final String STR_USER_ID = "user_id";
    private static final String STR_REQUEST_TIME = "request-time";
    private static final String STR_STATUS = "status";

    public static List<ProcessResult> processKafkaMsg(List<String> messageList) {
        List<ProcessResult> list = new ArrayList<ProcessResult>();
        for (String msg : messageList) {
            ProcessResult result = processKafkaMsg(msg);
            if (result != null) {
                list.add(result);
            }
        }
        return list;
    }

    public static ProcessResult processKafkaMsg(String message) {
        JsonObject object = new JsonObject(message);

        String userServiceLog = object.getString(STR_MESSAGE);
        ProcessResult result = null;
        try {
            result = processUserServiceLog(userServiceLog);

        } catch (Exception e) {
            logger.error("parse error", e);
            logger.error(message);
        }

        return result;
    }

    public static ProcessResult processUserServiceLog(String userServiceLog) throws Exception {
        int index = userServiceLog.indexOf('{');

        String accessLog = userServiceLog.substring(index);
        JsonObject log = new JsonObject(accessLog);

        String sessionString = log.getString(STR_SESSION);

        String uri = log.getString(STR_URI);
        String requestMethod = getRequestMethod(uri);

        String requestString = log.getString(STR_REQUEST);
        String responseString = log.getString(STR_RESPONSE);
        JsonObject responseObj = new JsonObject(responseString);
        String statusCode = "-1";
        if (responseObj != null) {
            statusCode = responseObj.getJsonObject(STR_STATUS).getString(STR_STATUS);
        }


        Long date = log.getLong(STR_REQUEST_TIME);

        ProcessResult result = null;
        if (uri.contains("/login")) {
            ObjectMapper mapper = new ObjectMapper();
            Request request = mapper.readValue(requestString, Request.class);

            String applicationId = request.getApplicationId();
            String credentialsType = null;
            String credentialsKey = null;
            String userId = null;
            String deviceId = null;
            Credential credential = request.getCredential();
            DeviceInfo deviceInfo = request.getDeviceInfo();
            if (credential != null) {
                credentialsKey = credential.getCredentialKey();
                credentialsType = credential.getCredentialType();
            }
            if (deviceInfo != null) {
                deviceId = deviceInfo.getDeviceUid();
            }
            JsonObject response = log.getJsonObject(STR_RESPONSE);
            if (response != null) {
                userId = response.getString(STR_USER_ID);
            }
            result = new ProcessResult(date, userId, applicationId, credentialsType, credentialsKey, deviceId, requestMethod,statusCode);

        } else if (uri.contains("/register")) {
            ObjectMapper mapper = new ObjectMapper();
            Request request = mapper.readValue(requestString, Request.class);

            String applicationId = request.getApplicationId();
            String credentialsType = null;
            String credentialsKey = null;
            String userId = null;
            String deviceId = null;
            Credential credential = request.getCredential();
            DeviceInfo deviceInfo = request.getDeviceInfo();
            if (credential != null) {
                credentialsKey = credential.getCredentialKey();
                credentialsType = credential.getCredentialType();
            }
            if (deviceInfo != null) {
                deviceId = deviceInfo.getDeviceUid();
            }

            JsonObject response = log.getJsonObject(STR_RESPONSE);
            if (response != null) {
                userId = response.getString(STR_USER_ID);
            }
            result = new ProcessResult(date, userId, applicationId, credentialsType, credentialsKey, deviceId, requestMethod,statusCode);
        } else {
            if (sessionString == null) {
                logger.debug(responseString);
                return null;
            }
            ObjectMapper mapper = new ObjectMapper();
            Session session = mapper.readValue(sessionString, Session.class);
            String applicationId = session.getApplicationId();
            String credentialsType = session.getCredentialsType();
            String credentialsKey = session.getCredentialsKey();
            String userId = session.getUserId();

            String deviceId = null;
            DeviceInfo deviceInfo = session.getDeviceInfo();
            if (deviceInfo != null) {
                deviceId = deviceInfo.getDeviceUid();
            }

            //if device_uid in session.device_info is null, try get device_uid from request.device_info
            if (deviceId == null) {
                Request request = mapper.readValue(requestString, Request.class);
                deviceInfo = request.getDeviceInfo();
                if (deviceInfo != null) {
                    deviceId = deviceInfo.getDeviceUid();
                }
            }

            result = new ProcessResult(date, userId, applicationId, credentialsType, credentialsKey, deviceId, requestMethod,statusCode);
        }

        return result;

    }

    private static String getRequestMethod(String uri) {
        if (uri == null) {
            return null;
        }

        int index0 = uri.indexOf('?');
        if (index0 == -1) {
            index0 = uri.length();
        }

        String simpleUri = uri.substring(0, index0);

        int index1 = simpleUri.lastIndexOf('/');
        if (index1 == -1) {
            return null;
        }
        return simpleUri.substring(index1, index0);

    }
}
