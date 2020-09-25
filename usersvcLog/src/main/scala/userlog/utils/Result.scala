package userlog.utils

case class Result(
                   time: Long,
                   userId: String,
                   applicationId: String,
                   credentialType: String,
                   credentialKey: String,
                   deviceId: String,
                   requestName: String,
                   statusCode: String)
