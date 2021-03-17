package df_groupby_agg

import org.apache.spark.sql.functions.collect_list

object DFGroupByAgg {

  def main(args: Array[String]): Unit = {
    // 有下面这样的DF
    /**
      * + --- + ------- + -------- +
      * | id | fName |名称|
      * + --- + ------- + -------- +
      * | 1 | Akash | Sethi |
      * | 2 |库纳尔|卡普尔|
      * | 3 |里沙卜|维尔马|
      * | 2 | Sonu | Mehrotra |
      * + --- + ------- + -------- +
      */
    // 要转换成下面的形式
    /**
      * + --- + ------- +- ------- + -------------------- +
      * | id | fname | lName |
      * + --- + ------- + -------- + -------------------- +
      * | 1 | [Akash] | [Sethi] |
      * | 2 | [Kunal，Sonu] | [Kapoor，Mehrotra] |
      * | 3 | [Rishabh] | [Verma] |
      * + --- + ------- + -------- + -------------------- +
      */
    df.groupBy(" id").agg(collect_list("fName"), collect_list("lName"))
  }

}
