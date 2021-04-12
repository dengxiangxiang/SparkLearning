package SaveReceiptRequest

case class ReceiptRequestInfo(requestTime:Long,
                              requestTimeString:String,  //timezone: GMT+0:00
                              applicationId: String,
                              secureToken: String,
                              deviceUid: String,
                              osName: String,
                              paymentProcessor: String,
                              offerExpiryUtcTimeStamp: Long,
                              purchaseTimeStamp: Long,
                              sku: String,
                              offerCode: String
                             )
