package userlog.utils

object UserUtils {
   val userServers: Array[String] = Array(
//    "ec1-usersvc-01",
//    "ec1-usersvc-02",
//    "ec1-usersvc-03",
//    "ec1-usersvc-04",
//    "ec2-usersvc-01",
//    "ec2-usersvc-02",
    "ec2-usersvc-03"
   )

  def getUserServers:Array[String] = this.userServers


}
