import os

HADOOP_HOME = r"C:\Users\SEEMANT\OneDrive\Desktop\intership\hadoop-win-utils"

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["hadoop.home.dir"] = HADOOP_HOME
os.environ["PATH"] = os.path.join(HADOOP_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

from data_setup import df, spark
from pyspark.sql.window import Window
from pyspark.sql.functions import col,round,lit, row_number,upper,lower,concat,avg,split,size,explode,array_contains,count,sum,collect_list,when,row_number,rank,dense_rank,udf
from pyspark.sql.types import StringType
base_df = df

# Q1 — Display only the name and age columns for all employees.
##df. show()
##df.select("age","name")

# Q3 — Display the name and salary of employees whose salary is less than ₹70,000.
##df.filter(col("salary")<70000).select("name","salary")

# Q4 — Display the id, name, and age of employees who are at least 25 years old.
##df.filter(col("age")>=25).select("id","name","age")

# Q5 — Display the name and salary of employees whose age is below 27.
##df.filter(col("age")>=27).select("name","salary").show()

# Q10 — Display the name, age, and salary of employees who are either younger than 25 or earning more than ₹80,000.
##df.filter(((col("age")<26)|(col("age")>28)) & (col("salary")>60000)).select("name","age","salary").show()

# Q12 — Display the name and salary of employees who are either younger than 25 and earning more than ₹55,000, OR 30 or older and earning more than ₹80,000.
##df.filter(((col("age")<25)&(col("salary")>55000))|((col("age")>=30)&(col("salary")>80000)))\
##.select("name","salary").show()

# Q13 — Display the name, age, and salary of employees who are between 24 and 30 years old inclusive and whose salary is not ₹65,000.
##df.filter((col("age").between(24,30))&(col("salary")!=65000))\
##.select("name","age","salary").show()

# Q14 — Create a high-potential employee report: age below 30, salary above ₹60,000, excluding salary exactly ₹70,000.
##df.filter((col("age")<30)&(col("salary")>60000)&(col("salary")!=70000))\
##.select("name","salary","age").show()

# Q17 — Create a new column containing salary after a 10% raise.
##df=df.withColumn("salary_after_10_percent_raise",round(col("salary")*1.10))
##df.select("*").show()

# Q18 — Create a new column showing each employee's age next year.
##df = df.withColumn("age_next_year",col("age")+1)
##df.select("*").show()

# Q19 — Create a new salary column after a 15% raise, keep employees above ₹75,000, and sort highest to lowest.
##df = df.withColumn("new_salary",round(col("salary")*1.15))
##df = df.filter(col("new_salary")>75000)
##df = df.sort(col("new_salary").desc())
##df.select("name","salary","new_salary").show()

# Q20 — Display all employees ordered from highest salary to lowest salary.
##df = df.sort(col("salary").desc())
##df.select("name","salary").show()

# Q21 — Display the 3 highest-paid employees, ordered from highest salary to lowest.
##df = df.sort(col("salary").desc())
##df = df.limit(3)
###df.select("name","salary").show()\

# Q22 — Create a bonus column equal to 5% of salary and display name, salary, and bonus.
##df = df.filter(col("salary")<70000)
##df = df.withColumn("bonus",(col("salary"))*1.05)
##df.select("name","salary","bonus").show()

# Q23 — Create an annual_bonus column equal to 8% of salary, sort by bonus highest to lowest, and keep the top 3.
##df = df.withColumn("annual_bonus",col("salary")*.08)
##df = df.sort(col("annual_bonus").desc())
##df = df.limit(3).show()

# Q24 — Find the top 2 employees among those earning less than ₹80,000, with a 12% bonus, sorted by bonus.
##df = df.where(col("salary")<80000)
##df = df.withColumn("bonus",col("salary")*0.12)
##df = df.sort(col("bonus").desc())
##df = df.limit(2)
##df.select("name","bonus").show()

# Q28 — Create a projected salary with a 20% increase, keep employees younger than 30, sort by projected salary, and show the top 2.
##df.withColumn("projected_salary",col("salary")*1.2)\
 ## .where(col("age")<30)\
 ## .sort(col("projected_salary").desc())\
##.limit(2)\
 ## .select("name","salary","projected_salary").show()

#Q29 -Your employee DataFrame has a column called salary.Rename it to monthly_salary without changing any data
##df.withColumnRenamed("salary","monthly_salary").show()

# Q30 - Rename name to employee_name and salary to monthly_salary; show employee_name, age and monthly_salary.
##df.withColumnRenamed("name","employee_name")\
  ##.withColumnRenamed("salary","monthly_salary")\
  ##.show()

# Q31  - Rename id, name and salary; keep age unchanged; display the renamed DataFrame sorted by salary.
##df.toDF("employee_id","employee_name","age","monthly_salary")\
  ##.show()

# Q32 - Drop the id column and display the remaining columns.
##df.drop("id").show()

# Q33 - Drop age and id, then display name and salary.
##df.drop("id","age").show()

# Q34 - Drop id and age, rename salary to monthly_salary, then show the top 3 highest-paid employees.
##df.withColumnRenamed("salary","monthly_salary")\
  ##.drop("id","age")\
  ##.sort(col("monthly_salary").desc())\
  ##.limit(3)\
  ##.show()

# Q35 - Cast salary to integer and display name, age and salary.
##df.withColumn("salary",col("salary").cast("int"))\
  ##.select("name","age","salary")\
  ##.show()

# Q36 - Remove duplicate ages and display the unique ages.
##df.dropDuplicates(["age"]).show()

# Q37 - Remove duplicate employees based on name, keeping all other columns.
##df.dropDuplicates(["name"]).show()

# Q38 - Drop duplicate employees based on name and age, keeping all other columns.
##df.dropDuplicates(["name","age"]).show()

# Q39 - Remove duplicate employees by name, then show the highest-paid record for each unique name.
##df.sort(col("salary").desc())\
  ##.dropDuplicates(["name"]).show()

##Union

##new_data = [
##    (21, "Aditya", 27, 75000, 102, "Analytics", "Delhi", "Python,SQL"),
 ##   (22, "Nisha", 29, 82000, 103, "HR", "Gurgaon", "Excel,Pytho"),
 ##   (23, "Varun", 24, 58000, 105, "Marketing", "Noida", "SEO,Excel")
#]

##new_df = spark.createDataFrame(new_data, [
   ## "name",
   ## "age",
   ## "salary",
   ## "department_id",
   ## "department",
   ## "location",
   ## "skills"
##])
# Q40 - Create a second employee DataFrame with 3 new employees and combine it with the main DataFrame using union().
##df.union(new_df).show(30)


# Q41 - Combine the main DataFrame with two additional employee DataFrames using union().
##Third_data = [
   ## (21, "Aditya", 27, 75000, 102, "Analytics", "Delhi", "Python,SQL"),
    ##(22, "Nisha", 29, 82000, 103, "HR", "Gurgaon", "Excel,Python")]
##Third_df = spark.createDataFrame(Third_data, [
    ##"employee_id",
    ## "name",
       ## "age",
       ## "salary",
       ## "department_id",
       ## "department",
       ## "location",
       ## "skills"])
##df.union(new_df)\
  ##.union(Third_df).show(30)

# Q42 - Filter Delhi employees from df and Gurgaon employees from new_df, then combine both using union().
##df = df.filter(col("location")== "Delhi")
##new_df = new_df.filter(col("location")=="Gurgaon")
##df.union(new_df).show()

# Q43 - Combine two DataFrames with the same columns in different orders using unionByName().
##four_data = [
  ##  (21, 75000 , 27, 102, "Analytics","Aditya" ,"Delhi", "Python,SQL"),
 ##       (22,82000,  29,  103, "HR","Nisha", "Gurgaon", "Excel,Python")]
##four_df = spark.createDataFrame(four_data, [
  ##  "employee_id",
  ##  "salary",
  ##  "age",
  ##  "department_id",
  ##  "department",
  ##  "name",
  ##  "location",
  ##  "skills"])
##df.unionByName(four_df).show()

# Q45 - Combine DataFrames with different column sets using unionByName() and preserve all columns.
##four_df=four_df.withColumn("new",lit("new"))
##df.unionByName(four_df,allowMissingColumns= True).show(30)

# Q46 - Convert all employee names to uppercase and display name.
##df.withColumn("name",upper(col("name"))).show()

# Q47 - Display employee names in lowercase and locations in uppercase with employee_id.
##df.withColumns({"name":lower(col("name")),
  ##"location":upper(col("location"))}).show()

# Q48 - Create an email column using each employee's name in lowercase, formatted as name@company.com, and show all columns.
##df.withColumn("email",concat(lower(col("name")),lit("@company.com")))\
  ##.show()

# Q49 - Create a company_id using lowercase department, "_", and employee_id; display name, department and company_id.
##df.withColumn("company_id",concat(lower(col("department")),lit("_"),col("employee_id")))\
  ##.select("name","department","company_id").show()

# Q50 - Create a username using lowercase name and employee_id; show name, employee_id and username.
##df.withColumn("username",concat(lower(col("name")),col("employee_id")))\
  ##.select("name","employee_id","username").show()

# Q51 - Find employees with missing salary values and display their name and salary.
##df.filter(col("salary").isNull())\
  ##.select("name","salary").show()

# Q52 - Replace missing salary values with 50000 and display all columns.
##df.fillna(50000,subset=("salary"))\
  ##.show()

# Q54 - Split the skills column into an array of individual skills.
##df.withColumn("skills",split("skills",","))\
  ##.show()

# Q55 - Split skills into an array and create a skill_count column showing the number of skills per employee.
##df.withColumn("skills",split("skills",","))\
  ##.withColumn("skill_count", size("skills"))\
  ##.show()

# Q56 - Explode the skills array so each skill appears in a separate column.
##df.withColumns({
    ##"skills":split("skills",",").getItem(1),
    ##"skills2":split("skills",",").getItem(0)
    ##}).show()

# Q57 - Explode the skills array so each skill appears in a separate row.
##df.withColumn("skills",split("skills",","))\
  ##.withColumn("skills",explode("skills")).show()

# Q58 - Explode skills and display only employee name and individual skill.
##df.withColumn("skills",split("skills",","))\
  ##.withColumn("skills",explode("skills"))\
  ##.select("name","skills").show()

# Q59 - Find employees whose skills include "Spark" and display their name and skills.
##df.withColumn("skills",split("skills",","))\
  ##.filter(array_contains(col("skills"),"Spark"))\
  ##.select("name","skills").show()

# Q60 - Find the number of employees in each department and display department and employee_count.
##df.groupBy("department").count().show()

# Q61 - For each department, calculate the average salary and show department and average_salary.
##df.groupBy("department").agg(avg("salary").alias("average_salary"))\
  ##.show()

# Q62 - For each department, calculate total salary, average salary and employee count.
##df.groupBy("department").agg(
   ##sum("salary").alias("total_salary"),
   ##avg("salary").alias("average_salary"),
   ##count(col("department")).alias("number_of_people")
   ##).show()

# Q63 - For each department, create a list of employee names.
##df.groupBy("department").agg(
    ##collect_list("name").alias("names")
##).show()

# Q64 - For each location, create a list of employee names working there.
##df.groupBy("location").agg(
    ##collect_list("name").alias("names")
##).show()

# Q65 - For each department, create a list of names of employees earning more than ₹65,000.
##df.filter(col("salary")>65000)\
  ##.groupBy("department").agg(
  ## collect_list("name").alias("names")
  ##).show()

# Q66 - Create a pivot table showing total salary for each department by location.
##df.groupBy("department").pivot("location").agg(
    ##sum("salary")
##).show()

# Q67 - Create a pivot table showing the average salary for each department by location.
##df.groupBy("department").pivot("location").agg(
    ##avg("salary")
##).show()

# Q68 - For employees aged 25 or older, create a pivot showing total salary by department and location.
##df.filter(col("age")>=25)\
  ##.groupBy("department").pivot("location").agg(
      ##sum("salary")
  ##).show()

# Q69 - Create a salary_category column: Low, Medium or High based on salary.
##df.withColumn("salary_category",when(col("salary")<60000,"low")\
##.when(col("salary")<=80000,"medium")\
##.otherwise("high"))\
##.show()

# Q70 - Create an age_category column: under 25 = Young, 25-29 = Adult, 30+ = Senior.
##df.withColumn("age_category",when(col("age")<25,"Young")\
##.when(col("age")<=29,"Adult")\
##.otherwise("Senior"))\
##.select("name","age","age_category").show()

# Q71 - Create employee_level using age and salary: Junior, Mid, Senior; display name, age, salary and employee_level.
##df.withColumn("employee_level",when((col("salary"))>=80000,"senior")
              ##.when((col("age")>=25) & (col("salary")<80000),"mid")
              ##.otherwise("junior"))\
  ##.select("name","age","salary","employee_level").show()

# Q72 - Create promotion_status: Immediate, Review or No Promotion based on age and salary.
##df.withColumn("promotion_status",when((col("salary")>=80000) & (col("age")>=28),"immediate")
              ##.when((col("salary")>=70000)|(col("age")>=30),"review")
              ##.otherwise("No promotion"))\
  ##.select("name","age","salary","promotion_status").show()

# Department DataFrame for join practice

department_data = [
    (101, "Engineering"),
    (102, "Analytics"),
    (103, "HR"),
    (104, "Finance"),
    (105, "Marketing")
]

department_df = spark.createDataFrame(
    department_data,
    ["department_id", "department_name"]
)
# Q73 - Inner join employees with departments using department_id; show employee name and department name.
##df.join(department_df,df["department_id"]==department_df["department_id"],"inner")\
##.select("name","department_name")\
##.show()

# Q74 - Left join employees with departments; show name, department_id and department_name.
##df.join(department_df,on = "department_id",how = "left")\
  ##.select("name","department_id","department_name").show()

# Q75 - Find employees whose department_id has no matching department record using a left anti join.
##df.join (department_df, on = "department_id",how = "anti")\
  ##.select("employee_id","name","department_id")\
  ##.show() 

# Q79 - Compare rank() and dense_rank() for employee salaries within each department.
##window = Window.partitionBy("department").orderBy(col("salary").desc())
##df.withColumn("salary_rank",rank().over(window))\
  ##.withColumn("dense_salary_rank",dense_rank().over(window))\
  ##.select("name","department","salary","salary_rank","dense_salary_rank")\
  ##.show()

# Q80 - Calculate cumulative salary within each department, ordered by employee_id.
##window = Window.partitionBy("department").orderBy(col("employee_id"))\
               ##.rowsBetween( Window.unboundedPreceding,Window.unboundedFollowing)
##df.withColumn("cumalative_sum",sum("salary")\
              ##.over(window))\
              ##.select("employee_id","name","department","salary","cumalative_sum")\
              ##.show()

# Q81 - Create a Python UDF to classify employees as "Young" (age < 25) or "Adult" (age >= 25); display name, age and age_group.

##def classify_age(age):
    ##if age < 25:
        ##return "young"
    ##else:
        ##return "adult"

##age_udf = udf(classify_age, StringType())

##df.withColumn("age_group", age_udf(col("age")))\
  ##.select("name","age","age_group")\
  ##.show()

# Q82 - Create a UDF to classify salaries: below 60000 = Low, 60000–80000 = Medium, above 80000 = High; display name, salary and salary_category.
##def salary_category(salary):
    ##if salary is None:
        ##return "Missing"
    ##if salary>80000:
        ##return "High"
    ##if salary>=60000:
        ##return "Medium"
    ##else:
        ##return "low"
##salary_udf = udf(salary_category, StringType())
##df.withColumn("salary_category",salary_udf(col("salary")))\
  ##.select("name","salary","salary_category")\
  ##.show()

# Q83 - Create a UDF to classify bonus eligibility using age and salary, handling missing salaries.
##def bonus_status(salary,age):
    ##if salary is None:
        ##return "salary missing"
    ##if salary>=80000 and age>=30:
        ##return "priority"
    ##if salary>=65000 and age>=25:
        ##return "Eligible"
    ##else:
        ##return "Not Eligible"
##bonus_udf = udf(bonus_status, StringType())
##df.withColumn("bonus_status", bonus_udf(col("salary"),col("age")))\
  ##.select("name","age","salary","bonus_status")\
  ##.show()

# Q84 - Write the employee DataFrame to a CSV output directory, including column headers.

##df.write \
##.mode("overwrite")\
##.option("header",True)\
##.cvs("output/employees_csv")

# Q85 - Write employee_id, name, department and salary to a Parquet output directory.

##df.select("employee_id", "name", "department", "salary") \
 ## .write \
  ##.mode("overwrite") \
 ## .parquet("output/employees_parquet")




##ETL pipeline questions


# Q1 - Clean employee data, remove duplicates and missing salaries,
# calculate a 10% raise, filter, sort and save the final result to Parquet.

##clean_df = base_df.dropna(subset=["salary"]) \
 ##   .dropDuplicates(["name", "age", "salary", "department_id"])

##final_df = clean_df.withColumn(
    ##"New_salary",
   ## col("salary") * 1.1
##).filter(
 ##   col("New_salary") > 70000
##).sort(
 ##   col("New_salary").desc()
##).select(
##    "employee_id",
 ##   "name",
 ##   "department",
 ##   "salary",
 ##   "New_salary"
##)
##final_df.write \
 ##   .mode("overwrite") \
 ##   .parquet("output/employees_parquet")
##saved_df = spark.read.parquet("output/employees_parquet")
##print("Final row count:", saved_df.count())
##saved_df.show(truncate=False)




# Q2 - Build a basic ETL pipeline for employee salary data.
# EXTRACT:
# Use the original employee DataFrame as your source.
# TRANSFORM:
# 1. Remove employees whose salary is missing.
# 2. Create a new column called annual_salary by multiplying
#    the monthly salary by 12.
# 3. Keep only employees whose annual_salary exceeds ₹8,00,000.
# LOAD:
# 4. Save the transformed data to "output/etl_q1" in Parquet format.
# 5. Read the saved Parquet data back into a new DataFrame.
# VALIDATE:
# 6. Print the final record count and display the saved records.

##final_df = base_df.dropna(subset = ["salary"])\
 ##                 .withColumn("annual_salary",(col("salary")*12))\
 ##                 .where(col("annual_salary")>800000)
##final_df.write\
 ##      .mode("overwrite")\
 ##      .parquet("output/employees_parquet")
##final_df = spark.read.parquet("output/employees_parquet")
##final_df.show()
##print("Final record count:", final_df.count())

# Q2 - Build an ETL pipeline to generate a department-wise salary report.
#
# EXTRACT:
# 1. Use the original employee DataFrame as the source.
#
# TRANSFORM:
# 2. Remove employees whose salary is missing.
# 3. Remove exact duplicate records.
# 4. Group employees by department.
# 5. Calculate the employee count, total salary and average salary
#    for each department.
# 6. Name the resulting columns:
#    department, employee_count, total_salary, average_salary
#
# LOAD:
# 7. Save the department-wise report as Parquet at "output/etl_q2".
#
# VALIDATE:
# 8. Read the saved Parquet data into a new DataFrame.
# 9. Print the final record count and display the saved report.

##new_df = df.dropna(subset=["salary"])\
##           .drop_duplicates()\
##           .groupBy("department").agg(
##                    count("*").alias("employee_count"),
##                    (sum("salary")).alias("total_salary"),
##                    (avg("salary")).alias("average_salary"),
##                    )
           
##new_df.write\
    ##.mode("overwrite")\
    ##.parquet("output/etl_q2")
##new_df= spark.read.parquet("output/etl_q2")
##print("Final record count:", new_df.count())
##new_df.show(truncate=False)




# Q3 - Build an ETL pipeline to validate employee data and categorize salaries.
#
# EXTRACT:
# 1. Use the original employee DataFrame as the source.
#
# TRANSFORM:
# 2. Remove employees with missing age or salary.
# 3. Keep only employees whose age is between 18 and 60, inclusive,
#    and whose salary is greater than zero.
# 4. Create a salary_category column:
#       salary >= 80000          -> "High"
#       salary >= 60000 and < 80000 -> "Medium"
#       salary < 60000           -> "Low"
# 5. Keep only these columns:
#    employee_id, name, age, department, salary, salary_category.
# 6. Sort employees by salary from highest to lowest.
#
# LOAD:
# 7. Save the transformed data as Parquet at "output/etl_q3".
#
# VALIDATE:
# 8. Read the saved data back into a new DataFrame.
# 9. Print the final record count and display the saved records.           
          

##new_df = df.dropna(subset = ["age","salary"])\
           ##.filter((col("age")).between(18,60) & (col("salary")>0))\
           ##.withColumn("salary_category",
                       ##when(col("salary")<60000,"low")\
                       ##.when(col("salary")<80000,"medium")\
                       ##.otherwise("high")
                      ##)\
           ##.select("employee_id","name","age","department","salary","salary_category")\
           ##.sort(col("salary").desc())

##new_df.write\
      ##.mode("overwrite")\
      ##.parquet("output/etl_q3")
##new_df= spark.read.parquet("output/etl_q3")
##print("Final record count:", new_df.count())
##new_df.show(truncate=False)     


# Q4 - Build an ETL pipeline to calculate employee bonuses
# and generate a department-wise bonus report.
#
# EXTRACT:
# 1. Use the original employee DataFrame as the source.
#
# TRANSFORM:
# 2. Remove employees with missing age or salary.
# 3. Keep employees aged 18–60 with a salary greater than zero.
# 4. Create a bonus_rate column:
#       Salary >= 80000 -> 10% bonus
#       Salary < 80000  -> 5% bonus
# 5. Calculate bonus_amount using the employee's salary and bonus rate.
# 6. Group the data by department.
# 7. Calculate:
#       - employee_count
#       - total_bonus
#       - average_salary
# 8. Sort departments by total_bonus, highest to lowest.
#
# LOAD:
# 9. Save the final report as Parquet at "output/etl_q4".
#
# VALIDATE:
# 10. Read the saved report into a new DataFrame.
# 11. Print the final record count and display the report.

new_df = df.dropna(subset = ["age","salary"])\
           .filter((col("age").between(18,60))&(col("salary")>0))\
           .withColumn("bonus_rate",
                       when(col("salary")>=80000,"10%")\
                       .otherwise("5%")
                       )\
           .withColumn("bonus_amount",
                       when(col("salary")>=80000,(col("salary")*0.1))\
                       .otherwise(col("salary")*0.05)
                       )\
           .groupBy("department").agg(
               count("*").alias("employee_count"),
               sum("bonus_amount").alias("total_bonus"),
               avg("salary").alias("average_salary"))\
           .sort(col("total_bonus").desc())
new_df.write\
      .mode("overwrite")\
      .parquet("output/etl_q4")
new_df= spark.read.parquet("output/etl_q4")
print("Final record count:", new_df.count())
new_df.show(truncate=False)



           

     
        

    
