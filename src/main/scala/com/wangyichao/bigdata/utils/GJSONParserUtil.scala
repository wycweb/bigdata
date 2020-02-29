package com.wangyichao.bigdata.utils

import java.sql.ResultSet

import com.google.gson.JsonParser

import scala.util.control.Breaks.{break, breakable}

object GJSONParserUtil {

  def checkString(value: Any): Any = {
    if (value.equals(null)) {
      value
    } else {
      if (value.toString.substring(0, 1).equals("\"") && value.toString.substring(value.toString.length - 1).equals("\"")) {
        "'" + value.toString.replaceAll("\"", "") + "'"
      } else {
        value
      }
    }
  }

  //解析JSON转为Phoenix SQL
  def parseJSONToPhoenixSQL(sqlstr: String) = {


    try {
      //字符串转换json,解析database,table,type,data,拼接Phoenix sql语句
      var sql = StringBuilder.newBuilder
      sql.append("error")

      val jsonObj = new JsonParser().parse(sqlstr).getAsJsonObject()

      val database = jsonObj.get("database").toString.toLowerCase.replaceAll("\"", "")
      val table = jsonObj.get("table").toString.toLowerCase.replaceAll("\"", "")
      val typeDML = jsonObj.get("type").toString.toLowerCase.replaceAll("\"", "")
      val dataElements = jsonObj.getAsJsonObject("data").entrySet().iterator()

      var key = ""
      var value: Any = null
      val keysStr = StringBuilder.newBuilder
      val valuesStr = StringBuilder.newBuilder

      //maxwell 是指maxwell软件的进程连接到MySQL的元数据库
      if (typeDML.equals("insert") || typeDML.equals("update")) {
        sql.clear()
        sql.append("upsert into ")

        while (dataElements.hasNext) {
          var element = dataElements.next()
          key = element.getKey
          value = checkString(element.getValue.toString.replaceAll("(\r\n|\r|\n|\n\r|'|\b|\f|\\|/|\t|\\\\|\\\\\\\\|[\\[\\]])", ""))
          keysStr.append(key).append(",") //列拼接
          valuesStr.append(value).append(",") //值拼接
        }

        //去除最后一个字符,
        keysStr.deleteCharAt(keysStr.length - 1)
        valuesStr.deleteCharAt(valuesStr.length - 1)

        //拼接Upsert
        sql.append(table.toUpperCase)
          .append("(" + keysStr.toString() + ") values(")
          .append(valuesStr.toString() + ")")

        sql

      } else {
        sql.clear()
        sql.append("error")
      }

      sql.toString()

    } catch {
      case e: Exception =>
        //记录错误的SQL
        println(e.getMessage)
        "error"
    }
  }
}
