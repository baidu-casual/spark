package main

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

//import spark.implicits._


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

  def temp(): Unit = {
    val department1 = new Department("123456", "Computer Science")
    val department2 = new Department("789012", "Mechanical Engineering")
    val department3 = new Department("345678", "Theater and Drama")
    val department4 = new Department("901234", "Indoor Recreation")

    val employee1 = new Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
    val employee2 = new Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
    val employee3 = new Employee("matei", null, "no-reply@waterloo.edu", 140000)
    val employee4 = new Employee(null, "wendell", "no-reply@princeton.edu", 160000)
    val employee5 = new Employee("michael", "jackson", "no-reply@neverla.nd", 80000)

    val departmentWithEmployees1 = new DepartmentWithEmployees(department1, Seq(employee1, employee2))
    val departmentWithEmployees2 = new DepartmentWithEmployees(department2, Seq(employee3, employee4))
    val departmentWithEmployees3 = new DepartmentWithEmployees(department3, Seq(employee5, employee4))
    val departmentWithEmployees4 = new DepartmentWithEmployees(department4, Seq(employee2, employee3))


    /*val departmentsWithEmployeesSeq1 = Seq(departmentWithEmployees1, departmentWithEmployees2)
    val df1 = departmentsWithEmployeesSeq1.toDF()
    display(df1)

    val departmentsWithEmployeesSeq2 = Seq(departmentWithEmployees3, departmentWithEmployees4)
    val df2 = departmentsWithEmployeesSeq2.toDF()
    display(df2)*/
  }
}

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark").setMaster("local");
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
    import spark.implicits._


    val data = Array("Gaurav", "Rohan", "Anuj", "Aryan", "Rahul", "Rohan", "Rohan", "Gaurav", "Karan")
    val distData = sc.parallelize(data)
    val pairs = distData.map(s => (s, 1))
    val count1 = pairs.reduceByKey((a, b) => a + b)

    println("\n\n\n\n\n")
    println("The Data is : "+distData.collect.foreach(println))

    println("\n\n\n\n\n")
    println("Word-Count : "+count1.collect.foreach(println))
    val df = count1.toDF("Word","WordCount")
    df.show()

    val file = sc.textFile("/home/xs107-bairoy/xenonstack/l2/module4/spark/files/test.txt")
    val words = file.flatMap(_.split(" "))
    val count2 = words.map(s => (s,1)).reduceByKey(_+_)
    
    println("\n\n\n\n\n")
    println("The Words are : "+words.collect.foreach(println))

    println("\n\n\n\n\n")
    println("Word-Count : "+count2.collect.foreach(println))
  }
}