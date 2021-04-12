package SaveReceiptRequest

import org.scalatest.FunSuite

class SaveReceiptRequestETLTest extends FunSuite {

  test("SaveReceiptRequestETL.parse"){
    val log = "[03:15:04.903] [http-bio-8080-exec-14457] [INFO ] - [API-CALL] - {\"content-type\":\"application/json\",\"x-tn-requesttimezone\":\"-0500\",\"x-tn-appsignature\":\"6f41c947-b116-4729-bfa7-cc0c86a8e698:1610363703:0f10a954719fa621d16618082667b7a5\",\"x-tn-requestid\":\"172a97ae-c8ce-496d-8366-473d0c787c5a\",\"x-tn-speed\":\"0.596499502658844\",\"x-tn-mobilecarrier\":\"\",\"x-forwarded-proto\":\"http\",\"x-tn-instanceid\":\"96cc7324-dd2c-38fe-ab3d-e00f0be48959\",\"x-tn-txname\":\"saveReceipt\",\"response\":{\"status\":{\"status\":13200}},\"request\":{\"service_target\":\"DEVICE_INFO\",\"application_signature\":\"6f41c947-b116-4729-bfa7-cc0c86a8e698:1610363703:0f10a954719fa621d16618082667b7a5\",\"receipt\":{\"payment_processor\":\"TELENAV\",\"purchase_utc_timestamp\":1605145905952,\"additional_info\":{},\"offer_version\":\"1\",\"offer_code\":\"SGL_three_year_free\",\"sku\":\"com.telenav.scoutgpslink.3yearfree\",\"receipt_archivable\":false,\"offer_start_utc_timestamp\":0,\"transaction_id\":\"\",\"receipt_id\":\"\",\"offer_expiry_utc_timestamp\":1699753905952,\"description\":\"Toyota 3 Year Free Trial\",\"receipt_data\":\"{\\\"receipt_archivable\\\":false,\\\"description\\\":\\\"Toyota 3 Year Free Trial\\\",\\\"premium_features\\\":[\\\"Video Projection\\\"],\\\"level\\\":100,\\\"name\\\":\\\"Toyota 3 Year Free Trial\\\",\\\"offer_code\\\":\\\"SGL_three_year_free\\\",\\\"offer_version\\\":\\\"1\\\",\\\"pay_period\\\":{\\\"value\\\":\\\"3\\\",\\\"unit\\\":\\\"YEAR\\\"},\\\"grace_period\\\":{\\\"value\\\":\\\"1\\\",\\\"unit\\\":\\\"DAY\\\"},\\\"payment_processor\\\":\\\"TELENAV\\\",\\\"payment_type\\\":\\\"NO_CHARGE\\\",\\\"price\\\":{\\\"value\\\":\\\"0\\\",\\\"iso_code\\\":\\\"USD\\\"},\\\"sku\\\":\\\"com.telenav.scoutgpslink.3yearfree\\\",\\\"map_source\\\":\\\"OSM\\\",\\\"poi_source\\\":\\\"Factual\\\",\\\"address_source\\\":\\\"Telenav\\\",\\\"traffic_source\\\":\\\"clearchannel\\\",\\\"paymentGateway\\\":[]}\",\"application_id\":\"6f41c947-b116-4729-bfa7-cc0c86a8e698\"},\"application_id\":\"6f41c947-b116-4729-bfa7-cc0c86a8e698\",\"secure_token\":\"i556:7TPM4DE5H8WANRAM6YMGXDH3P/2CWH04Y218RSV8XQ4WSKYTLLW:3NUKUMAV6I2D9F2SYWWJQO623:6144280285201:rnor8270n79p1490n844o5s39p1r6524\",\"device_info\":{\"mac_address\":\"94:B2:CC:6C:E3:AA\",\"model\":\"100.1.T0800480170NA301200000\",\"os_name\":\"100.1.T0800480170NA301200000\",\"os_version\":\"1.1.000046\",\"device_uid\":\"94:B2:CC:6C:E3:AA\"}},\"x-tn-heading\":\"206.7955780029297\",\"x-tn-txid\":\"d849bbe3-f294-42fd-9ced-4958a2dad66a\",\"accept-encoding\":\"gzip\",\"x-tn-requesttimestamp\":\"1610363703755\",\"x-tn-mobileos\":\"ANDROID\",\"connection\":\"keep-alive\",\"host\":\"usersvc.mypna.com:8080\",\"x-forwarded-for\":\"47.14.76.245, 10.191.100.23\",\"x-forwarded-port\":\"8080\",\"x-tn-requestname\":\"saveReceipt\",\"x-tn-position\":\"42.4245302,-71.9008369\",\"x-tn-securetoken\":\"v001:2GCZ9QR0U3JNAENZ1LZTKQU8C/7PJU59L763EFI3KD9JFXLGYYJ:8AHXHZNI1V7Q4S7FLJJWDB178:1699735730756:eabe3725a24c6945a399b0f84c6e1079\",\"user-agent\":\"Apache-HttpClient/4.5.2 (Java/1.8.0_161)\",\"x-tn-txtimestamp\":\"1610363703755\",\"uri\":\"http://usersvc.mypna.com:8080/usersvc_v6/v6/receipt\",\"x-tn-connectiontype\":\"Wifi\",\"x-tn-appversion\":\"1.0.1006952\",\"breadcrumbid\":\"ID-ec1-nautilus-02-mypna-com-44234-1588214896850-0-160230230\",\"x-tn-deviceuid\":\"96cc7324-dd2c-38fe-ab3d-e00f0be48959\",\"session\":{\"session_id\":\"2GCZ9QR0U3JNAENZ1LZTKQU8C/7PJU59L763EFI3KD9JFXLGYYJ\",\"scope\":[],\"credentials_type\":\"ANONYMOUS\",\"login_timestamp\":\"1605127730616\",\"credentials_key\":\"n9861nr5-94o8-9o27-or8n-o752623r49q4\",\"user_id\":\"2GCZ9QR0U3JNAENZ1LZTKQU8C\",\"session_attributes\":{},\"application_id\":\"6f41c947-b116-4729-bfa7-cc0c86a8e698\",\"device_info\":{\"mac_address\":\"02:00:00:00:00:00\",\"model\":\"LGL722DL\",\"os_name\":\"ANDROID\",\"os_version\":\"9\",\"device_uid\":\"96cc7324-dd2c-38fe-ab3d-e00f0be48959\"},\"csr_id\":\"7TPM4DE5H8WANRAM6YMGXDH3P\"},\"content-length\":\"1620\",\"x-tn-userid\":\"2GCZ9QR0U3JNAENZ1LZTKQU8C\",\"request-time\":1610363704890,\"method\":\"POST\",\"x-tn-appid\":\"6f41c947-b116-4729-bfa7-cc0c86a8e698\",\"execution-time\":12}\n[03:29:01.639] [http-bio-8080-exec-14482] [INFO ] - [API-CALL] - {\"content-type\":\"application/json\",\"x-tn-requesttimezone\":\"-0500\",\"x-tn-appsignature\":\"6f41c947-b116-4729-bfa7-cc0c86a8e698:1610364541:087645651a3ddd57ef7208010791df80\",\"x-tn-requestid\":\"ab35922f-8a9c-4401-8175-ddab89697a7a\",\"x-tn-speed\":\"0.0\",\"x-tn-mobilecarrier\":\"Verizon\",\"x-forwarded-proto\":\"http\",\"x-tn-instanceid\":\"ac9bdc37-590a-36e3-9ff0-fce1012ffc88\",\"x-tn-txname\":\"saveReceipt\",\"response\":{\"status\":{\"status\":13200}},\"request\":{\"service_target\":\"DEVICE_INFO\",\"application_signature\":\"6f41c947-b116-4729-bfa7-cc0c86a8e698:1610364541:087645651a3ddd57ef7208010791df80\",\"receipt\":{\"payment_processor\":\"TELENAV\",\"purchase_utc_timestamp\":1605401082519,\"additional_info\":{},\"offer_version\":\"1\",\"offer_code\":\"SGL_three_year_free\",\"sku\":\"com.telenav.scoutgpslink.3yearfree\",\"receipt_archivable\":false,\"offer_start_utc_timestamp\":0,\"transaction_id\":\"\",\"receipt_id\":\"\",\"offer_expiry_utc_timestamp\":1700009082519,\"description\":\"Toyota 3 Year Free Trial\",\"receipt_data\":\"{\\\"receipt_archivable\\\":false,\\\"description\\\":\\\"Toyota 3 Year Free Trial\\\",\\\"premium_features\\\":[\\\"Video Projection\\\"],\\\"level\\\":100,\\\"name\\\":\\\"Toyota 3 Year Free Trial\\\",\\\"offer_code\\\":\\\"SGL_three_year_free\\\",\\\"offer_version\\\":\\\"1\\\",\\\"pay_period\\\":{\\\"value\\\":\\\"3\\\",\\\"unit\\\":\\\"YEAR\\\"},\\\"grace_period\\\":{\\\"value\\\":\\\"1\\\",\\\"unit\\\":\\\"DAY\\\"},\\\"payment_processor\\\":\\\"TELENAV\\\",\\\"payment_type\\\":\\\"NO_CHARGE\\\",\\\"price\\\":{\\\"value\\\":\\\"0\\\",\\\"iso_code\\\":\\\"USD\\\"},\\\"sku\\\":\\\"com.telenav.scoutgpslink.3yearfree\\\",\\\"map_source\\\":\\\"OSM\\\",\\\"poi_source\\\":\\\"Factual\\\",\\\"address_source\\\":\\\"Telenav\\\",\\\"traffic_source\\\":\\\"clearchannel\\\",\\\"paymentGateway\\\":[]}\",\"application_id\":\"6f41c947-b116-4729-bfa7-cc0c86a8e698\"},\"application_id\":\"6f41c947-b116-4729-bfa7-cc0c86a8e698\",\"secure_token\":\"i556:6IZL8NO343NBCROGP43KRP18V/GG7X2NCMDGPDR0PKFFO35BAE:1X75QDS5P2N9BB4YBHKVCXME3:6144445379001:2780ss7793so93nn3q6s9737n4prr1s1\",\"device_info\":{\"mac_address\":\"94:B2:CC:3E:B7:89\",\"model\":\"100.1.T0800480170NA201000000\",\"os_name\":\"100.1.T0800480170NA201000000\",\"os_version\":\"1.1.000046\",\"device_uid\":\"94:B2:CC:3E:B7:89\"}},\"x-tn-heading\":\"0.0\",\"x-tn-txid\":\"adc86b16-3828-42af-93d9-4c4ffea15c1c\",\"accept-encoding\":\"gzip\",\"x-tn-requesttimestamp\":\"1610364541801\",\"x-tn-mobileos\":\"ANDROID\",\"connection\":\"keep-alive\",\"host\":\"usersvc.mypna.com:8080\",\"x-forwarded-for\":\"174.248.22.77, 10.191.100.77\",\"x-forwarded-port\":\"8080\",\"x-tn-requestname\":\"saveReceipt\",\"x-tn-position\":\"40.0379156,-74.2371701\",\"x-tn-securetoken\":\"v001:1VMY3AB898AOPEBTC98XEC63I/TT2K7APZQTCQE5CXSSB80ONR:6K20DQF0C7A4OO9LOUXIPKZR8:1699990824556:7235ff2248fb48aa8d1f4282a9cee6f6\",\"user-agent\":\"Apache-HttpClient/4.5.2 (Java/1.8.0_161)\",\"x-tn-txtimestamp\":\"1610364541801\",\"uri\":\"http://usersvc.mypna.com:8080/usersvc_v6/v6/receipt\",\"x-tn-connectiontype\":\"Cellular\",\"x-tn-appversion\":\"1.0.1006952\",\"breadcrumbid\":\"ID-ec1-autodenalitrafficserver-levveltest-mypna-com-38102-1588215164813-0-160267708\",\"x-tn-deviceuid\":\"ac9bdc37-590a-36e3-9ff0-fce1012ffc88\",\"session\":{\"session_id\":\"1VMY3AB898AOPEBTC98XEC63I/TT2K7APZQTCQE5CXSSB80ONR\",\"scope\":[],\"credentials_type\":\"ANONYMOUS\",\"login_timestamp\":\"1605382824409\",\"credentials_key\":\"4n1s5689-3n5q-92n7-30p4-3760srq856n2\",\"user_id\":\"1VMY3AB898AOPEBTC98XEC63I\",\"session_attributes\":{},\"application_id\":\"6f41c947-b116-4729-bfa7-cc0c86a8e698\",\"device_info\":{\"mac_address\":\"02:00:00:00:00:00\",\"model\":\"SM-G986U\",\"os_name\":\"ANDROID\",\"os_version\":\"10\",\"device_uid\":\"ac9bdc37-590a-36e3-9ff0-fce1012ffc88\"},\"csr_id\":\"6IZL8NO343NBCROGP43KRP18V\"},\"content-length\":\"1619\",\"x-tn-userid\":\"1VMY3AB898AOPEBTC98XEC63I\",\"request-time\":1610364541625,\"method\":\"POST\",\"x-tn-appid\":\"6f41c947-b116-4729-bfa7-cc0c86a8e698\",\"execution-time\":13}"
    val info1: ReceiptRequestInfo = SaveReceiptRequestETL.parse(log)
  }

  test("SaveReceiptRequestETL.parse2"){
    val log = "{\n\t\"content-type\": \"application/json\",\n\t\"x-tn-appsignature\": \"c1761436-efe6-456c-95ac-4048d70ce829:1599230152575:21326be3837591c37f278a99b6ee3806\",\n\t\"x-tn-requestid\": \"6378CC3A-F49D-424D-97BA-454F1780A040\",\n\t\"x-tn-speed\": \"0.111300\",\n\t\"accept\": \"*/*\",\n\t\"x-tn-mobilecarrier\": \"Sprint\",\n\t\"x-forwarded-proto\": \"http\",\n\t\"x-tn-instanceid\": \"BAE37459-3D91-4FF8-83E1-854BB6616F56\",\n\t\"x-tn-txname\": \"SaveReceipt\",\n\t\"response\": {\n\t\t\"status\": {\n\t\t\t\"status\": 13200\n\t\t}\n\t},\n\t\"request\": {\n\t\t\"application_signature\": \"c1761436-efe6-456c-95ac-4048d70ce829:1599230152575:21326be3837591c37f278a99b6ee3806\",\n\t\t\"receipt\": {\n\t\t\t\"payment_processor\": \"TELENAV\",\n\t\t\t\"offer_expiry_utc_timestamp\": 94953600000,\n\t\t\t\"purchase_utc_timestamp\": 0,\n\t\t\t\"additional_info\": {\n\t\t\t\t\"ServiceTarget\": \"DEVICE_INFO\"\n\t\t\t},\n\t\t\t\"description\": \"Toyota 3 Year Free Trial\",\n\t\t\t\"offer_version\": \"1\",\n\t\t\t\"receipt_data\": \"\",\n\t\t\t\"offer_code\": \"SGL_three_year_free\",\n\t\t\t\"sku\": \"com.telenav.scoutgpslink.3yearfree\",\n\t\t\t\"application_id\": \"c1761436-efe6-456c-95ac-4048d70ce829\",\n\t\t\t\"receipt_archivable\": false,\n\t\t\t\"transaction_id\": \"\"\n\t\t},\n\t\t\"application_id\": \"c1761436-efe6-456c-95ac-4048d70ce829\",\n\t\t\"secure_token\": \"v001:EZWD7BTQSTQ23RLNX2IHEP0QZ/1N13YYR2TFJ9SPJOAKVEWU0LR:7GS01IE6CF8HJMM3HH2C01H87:1687005997451:9b1efd1ff4877c4563ba18c43a941817\",\n\t\t\"device_info\": {\n\t\t\t\"mac_address\": \"BAE37459-3D91-4FF8-83E1-854BB6616F56\",\n\t\t\t\"model\": \"iPhone\",\n\t\t\t\"os_name\": \"iOS\",\n\t\t\t\"make\": \"Apple\",\n\t\t\t\"device_uid\": \"94:B2:CC:3F:1F:BB\",\n\t\t\t\"os_version\": \"13.6.1\",\n\t\t\t\"instance_id\": \"BAE37459-3D91-4FF8-83E1-854BB6616F56\"\n\t\t}\n\t},\n\t\"x-tn-txid\": \"4481D57A-EC70-401C-9E5D-027AE6A97B4E\",\n\t\"x-tn-heading\": \"348.000000\",\n\t\"accept-encoding\": \"gzip\",\n\t\"x-tn-deviceinstanceid\": \"BAE37459-3D91-4FF8-83E1-854BB6616F56\",\n\t\"x-tn-requesttimestamp\": \"2020-09-04T7:35:52-0700\",\n\t\"x-tn-mobileos\": \"iOS\",\n\t\"connection\": \"keep-alive\",\n\t\"accept-language\": \"es-MX;q=1\",\n\t\"host\": \"usersvc.mypna.com:8080\",\n\t\"x-forwarded-for\": \"99.203.10.59, 10.188.104.119\",\n\t\"x-forwarded-port\": \"8080\",\n\t\"x-tn-requestname\": \"SaveReceipt\",\n\t\"x-tn-api_key\": \"c1761436-efe6-456c-95ac-4048d70ce829\",\n\t\"x-tn-securetoken\": \"v001:EZWD7BTQSTQ23RLNX2IHEP0QZ/1N13YYR2TFJ9SPJOAKVEWU0LR:7GS01IE6CF8HJMM3HH2C01H87:1687005997451:9b1efd1ff4877c4563ba18c43a941817\",\n\t\"x-tn-position\": \"32.549662,-116.938303\",\n\t\"x-tn-txtimestamp\": \"2020-09-04T7:35:52-0700\",\n\t\"user-agent\": \"ScoutFree/1.0.119 (iPhone; iOS 13.6.1; Scale/3.00)\",\n\t\"uri\": \"http://usersvc.mypna.com:8080/usersvc_v6/v6/receipt\",\n\t\"x-tn-connectiontype\": \"Cellular\",\n\t\"x-tn-appversion\": \"1.0.119.1850\",\n\t\"breadcrumbid\": \"ID-ec2-nautilus-07-mypna-com-40520-1588226028337-0-128234791\",\n\t\"session\": {\n\t\t\"session_id\": \"EZWD7BTQSTQ23RLNX2IHEP0QZ/1N13YYR2TFJ9SPJOAKVEWU0LR\",\n\t\t\"scope\": [],\n\t\t\"login_timestamp\": \"1592397997297\",\n\t\t\"status\": {\n\t\t\t\"status\": 20200\n\t\t},\n\t\t\"credentials_type\": \"ANONYMOUS\",\n\t\t\"credentials_key\": \"C6CB9E2F-2D07-4545-8570-E7D3439BB850\",\n\t\t\"user_id\": \"EZWD7BTQSTQ23RLNX2IHEP0QZ\",\n\t\t\"application_id\": \"c1761436-efe6-456c-95ac-4048d70ce829\",\n\t\t\"session_attributes\": {},\n\t\t\"csr_id\": \"EZWD7BTQSTQ23RLNX2IHEP0QZ\",\n\t\t\"device_info\": {\n\t\t\t\"mac_address\": \"BAE37459-3D91-4FF8-83E1-854BB6616F56\",\n\t\t\t\"model\": \"iPhone\",\n\t\t\t\"os_name\": \"iOS\",\n\t\t\t\"make\": \"Apple\",\n\t\t\t\"instance_id\": \"BAE37459-3D91-4FF8-83E1-854BB6616F56\",\n\t\t\t\"device_uid\": \"BAE37459-3D91-4FF8-83E1-854BB6616F56\",\n\t\t\t\"os_version\": \"13.3.1\"\n\t\t}\n\t},\n\t\"x-tn-deviceuid\": \"BAE37459-3D91-4FF8-83E1-854BB6616F56\",\n\t\"content-length\": \"960\",\n\t\"request-time\": 1599230176053,\n\t\"x-tn-userid\": \"EZWD7BTQSTQ23RLNX2IHEP0QZ\",\n\t\"x-tn-appid\": \"c1761436-efe6-456c-95ac-4048d70ce829\",\n\t\"method\": \"POST\",\n\t\"x-tn-api_signature\": \"c1761436-efe6-456c-95ac-4048d70ce829:1599230152575:21326be3837591c37f278a99b6ee3806\",\n\t\"execution-time\": 83\n}"
    val info1: ReceiptRequestInfo = SaveReceiptRequestETL.parse(log)
  }

  test("SaveReceiptRequestETLTest1") {
    val args = Array(
      "/Users/xxdeng/Desktop/usersvc-log-2020",
      "/Users/xxdeng/Documents/study/sparkParent/usersvcLog/src/test/resources/SaveReceiptRequestETLTest/output/202009",
      "2020-09-04,2020-09-05",
      "true",
      "true")
    SaveReceiptRequestETL.main(args)
  }
}