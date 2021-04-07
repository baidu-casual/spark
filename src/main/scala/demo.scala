package main


import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

case class Department(id: String, name: String)
case class Employee(firstName: String, lastName: String, email: String, salary: Int)
case class DepartmentWithEmployees(department: Department, employees: Seq[Employee])

class institute{


  def createEmployee(firstName: String, lastName: String, email: String, salary: Int): Employee = {
    val employee = new Employee(firstName, lastName, email, salary)
    employee
  }

  def createBranch(id: String, name: String): Department = {
    val department = new Department(id, name)
    department
  }


  def assignBranch(department: Department, employees: Seq[Employee]): DepartmentWithEmployees = {
    val assign = new DepartmentWithEmployees(department, employees)
    assign
  }

  def temp1(): Unit = {
    val conf = new SparkConf().setAppName("Spark").setMaster("local");
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Spark SQL").config(conf).getOrCreate()
    import spark.implicits._

    val department1 = new Department("123456", "Computer Science")
    val department2 = new Department("789012", "Mechanical Engineering")
    val department3 = new Department("345678", "Theater and Drama")
    val department4 = new Department("901234", "Indoor Recreation")
    val department = Seq(department1,department2,department3,department4)
    val dfDepartment = department.toDF()
    dfDepartment.show()

    val employee1 = new Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
    val employee2 = new Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
    val employee3 = new Employee("matei", null, "no-reply@waterloo.edu", 140000)
    val employee4 = new Employee(null, "wendell", "no-reply@princeton.edu", 160000)
    val employee5 = new Employee("michael", "jackson", "no-reply@neverla.nd", 80000)
    val employee = Seq(employee1,employee2,employee3,employee4,employee5)
    val dfEmployee = employee.toDF()
    dfEmployee.show()


    val departmentWithEmployees1 = new DepartmentWithEmployees(department1, Seq(employee1, employee2))
    val departmentWithEmployees2 = new DepartmentWithEmployees(department2, Seq(employee3, employee4))
    val departmentWithEmployees3 = new DepartmentWithEmployees(department3, Seq(employee5, employee4))
    val departmentWithEmployees4 = new DepartmentWithEmployees(department4, Seq(employee2, employee3))


    val departmentsWithEmployeesSeq1 = Seq(departmentWithEmployees1, departmentWithEmployees2)
    val df1 = departmentsWithEmployeesSeq1.toDF()
    df1.show()

    val departmentsWithEmployeesSeq2 = Seq(departmentWithEmployees3, departmentWithEmployees4)
    val df2 = departmentsWithEmployeesSeq2.toDF()
    df2.show()
  }
  def temp2(): Unit = {
    val conf = new SparkConf().setAppName("Spark").setMaster("local");
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Spark SQL").config(conf).getOrCreate()
    import spark.implicits._

    val data = Array("Gaurav", "Rohan", "Anuj", "Aryan", "Rahul", "Rohan", "Rohan", "Gaurav", "Karan")
    val distData = sc.parallelize(data)
    val pairs = distData.map(s => (s, 1))
    val count1 = pairs.reduceByKey((a, b) => a + b)

    println("\n\n\n\n\n")
    println("The Data is : "+distData.collect.foreach(println))

    println("\n\n\n\n\n")
    println("Word-Count : "+count1.collect.foreach(println))

    println("\n\n\n\n\n")
    val df1 = count1.toDF("Name","Name-Count")
    df1.show()
    df1.printSchema()
    df1.select("Name").show()
    //df1.select($"Name", $"age" + 1).show()
    //df1.filter($"age" > 21).show()
    //df1.groupBy("age").count().show()


    val file = sc.textFile("/home/xs107-bairoy/xenonstack/l2/module4/spark/files/test.txt")
    val words = file.flatMap(_.split(" "))
    val count2 = words.map(s => (s,1)).reduceByKey(_+_)
    
    println("\n\n\n\n\n")
    println("The Words are : "+words.collect.foreach(println))

    println("\n\n\n\n\n")
    println("Word-Count : "+count2.collect.foreach(println))

    println("\n\n\n\n\n")
    val df2 = count2.toDF("Word","Word-Count")
    df2.show()

    df2.createOrReplaceTempView("Word")
    val sqlDF2 = spark.sql("SELECT * FROM Word where `Word-Count`>1")
    sqlDF2.show()
  }
  def temp3(): Unit = {
    val conf = new SparkConf().setAppName("Spark").setMaster("local");
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Spark SQL").config(conf).getOrCreate()
    import spark.implicits._

    val path = "/home/xs107-bairoy/xenonstack/l2/module4/spark/files/data.csv"
    val df1 = spark.read.option("header", "true").format("csv").load(path).toDF()

    df1.createOrReplaceTempView("Orders")
    spark.sql("SELECT * FROM Orders").show()
    spark.sql("SELECT * FROM Orders where status='pending'").show()
    spark.sql("SELECT * FROM Orders where total>40").show()
  }
}

//
object demo extends institute {
  def init(): Unit = {
    val conf = new SparkConf().setAppName("Spark").setMaster("local");
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Spark SQL").config(conf).getOrCreate()
    import spark.implicits._
  }
  def main(args: Array[String]): Unit = {
    //init()
    val institute1 = new institute
    //institute1.temp1()
    //institute1.temp2()
    institute1.temp3()
  }
}