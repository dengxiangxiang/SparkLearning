package utils

case class Result(
                   time: Long,
                   userId: String,
                   applicationId: String,
                   credentialType: String,
                   credentialKey: String,
                   deviceId: String,
                   requestName: String,
                   statusCode: String)

case class BriefDataResult(
                            applicationId: String,
                            userId: String,
                            credentialType: String,
                            credentialKey: String,
                            deviceId: String,
                            count: Int)

case class RegisterDataResult(
                               applicationId: String,
                               userId: String,
                               credentialType: String,
                               credentialKey: String,
                               deviceId: String,
                               statusCode: Int,
                               count: Int)



