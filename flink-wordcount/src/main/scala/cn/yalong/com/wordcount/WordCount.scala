package cn.yalong.com.wordcount

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * Scala版本入门程序
 *
 * @author yalong.zhou
 *
 */
object WordCount {

  def main(args: Array[String]): Unit = {

    //Creates an execution environment
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //read content of file
    val content: DataSet[String] = environment.readTextFile("./data/student.txt")

    //切分单词
    val words: DataSet[String] = content.flatMap(_.toLowerCase.split(" ").filter(_.nonEmpty))

    //组成元组
    val wordAndOne: DataSet[(String, Int)] = words.map((_,1))

    //根据单词进行分组，并统计单词出现的次数
    val count: AggregateDataSet[(String, Int)] = wordAndOne.groupBy(0).sum(1)

    //打印输出的结果
    println(count.collect().toBuffer)

    //count.writeAsText("F:\\data\\student1.txt",WriteMode.OVERWRITE)

    count.writeAsCsv("F:\\data\\student1.csv","\n", " ")
  }
}
