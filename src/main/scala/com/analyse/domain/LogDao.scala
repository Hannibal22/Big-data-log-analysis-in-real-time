package com.analyse.domain

import com.analyse.site.HBase
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer


object LogDao {
  val tableName = "log"
  val cf = "info"
  val qualifer = "site_count"

  /**
    * 保存数据到HBase
    */
  def save(list: ListBuffer[CategaryClickCount]): Unit = {
    val table = HBase.getInstance().getHtable(tableName)
    for (els <- list) {
      table.incrementColumnValue(Bytes.toBytes(els.day_categaryId), Bytes.toBytes(cf), Bytes.toBytes(qualifer), els.click_count)
    }
  }

  /**
    * 根据rowkey查询值
    *
    */
  def count(day_categary: String): Long = {
    val talbe = HBase.getInstance().getHtable(tableName)
    val get = new Get(Bytes.toBytes(day_categary))
    val value = talbe.get(get).getValue(cf.getBytes(), qualifer.getBytes())
    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[CategaryClickCount]
    list.append(CategaryClickCount("20180517_3", 300))
    list.append(CategaryClickCount("20180517_4", 500))
    list.append(CategaryClickCount("20180517_5", 1000))
    save(list)

    print(count("20180517_3") + "---" + count("20180517_4")+ "---"+ count("20180517_5"))


  }


}
