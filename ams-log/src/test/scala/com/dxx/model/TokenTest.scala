package com.dxx.model

import com.dxx.analysis.AmsLogMain
import org.scalatest.FunSuite

class TokenTest extends FunSuite{

  test("token"){
    val token = "v002.n9s19257-61qr-90p0-3q91-8104064n537p/ybnq_grfg_hfre.a4f64702-16de-45c5-8d46-3659519a082c.1619776297107.509138c0f8dcff80df41bd01035fec38";
    val token1: AccessToken = AccessToken.parse(token)
  }

  test("getJsonString1"){
    val token = "10.191.100.145 - - [23/Apr/2021:00:00:00 -0700] \"GET /ams/monitor/ping HTTP/1.1\" 200 65 0";
    val s = AmsLogMain.getJsonString(token)
    print(s)

  }
  test("getJsonString2"){
    val token = "10.191.100.170 - - [23/Apr/2021:00:00:00 -0700] \"GET /ams/v1/validate/json?api_key=key&api_signature=sig&access_token=token HTTP/1.1\" 200 267 1";
    val s = AmsLogMain.getJsonString(token)
    print(s)

  }
  test("getJsonString3"){
    val token = " 10.191.100.170 - - [23/Apr/2021:00:00:00 -0700] \"GET /ams/v1/validate/json?api_key=key&api_signature=sig&access_token= HTTP/1.1\" 200 267 1";
    val s = AmsLogMain.getJsonString(token)
    print(s)

  }
  test("getJsonString4"){
    val token = "10.191.100.145 - - [23/Apr/2021:03:11:08 -0700] \"GET /ams/v1/validate/user?secure_token=v001:6OLJT434RI1ND0X90OXZILB3U/2FABF619LZUMNX9LN5XRZGW6K:EI7T584NPI57OLL7VNYTRB0KT:1694884850600:d4931ca94f9ba090eec8d7531700390d HTTP/1.1\" 200 541 1";
    val s = AmsLogMain.getJsonString(token)
    print(s)

  }


}
