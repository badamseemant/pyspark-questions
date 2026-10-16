import os

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\SEEMANT\AppData\Local\Programs\Python\Python39\python.exe"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySparkPractice") \
    .master("local[2]") \
    .getOrCreate()

data = [
    (1, "Aman",   22, 50000, 101, "Engineering", "Delhi",    "Python,SQL"),
    (2, "Rahul",  25, 70000, 102, "Analytics",   "Noida",    "Python,Excel,SQL"),
    (3, "Priya",  24, 60000, 101, "Engineering", "Delhi",    "Java,Python"),
    (4, "Neha",   28, 90000, 103, "HR",           "Gurgaon",  "Excel,HR"),
    (5, "Rohit",  30, 85000, 104, "Finance",      "Mumbai",   "SQL,Excel"),
    (6, "Simran", 26, 65000, 102, "Analytics",    "Noida",    "Python,Spark"),
    (7, "Aman",   27, 62000, 101, "Engineering",  "Delhi",    "Python,Spark"),
    (8, "Karan",  29, 78000, 102, "Analytics",    "Delhi",    "SQL,Spark"),
    (9, "Priya",  24, 61000, 101, "Engineering",  "Delhi",    "Java,Python"),
    (10, "Vikas", 31, 95000, 104, "Finance",      "Mumbai",   "SQL,Python"),
    (11, "Meera", 27, 72000, 103, "HR",           "Gurgaon",  "Excel,HR"),
    (12, "Arjun", 23, 55000, 105, "Marketing",    "Noida",    "SEO,Excel"),
    (13, "Karan",  29, 82000, 102, "Analytics",   "Delhi",    "SQL,Spark"),
    (14, "Isha",  26, None,  105, "Marketing",    "Noida",    "SEO,Python"),
    (15, "Vikas", 31, 95000, 104, "Finance",      "Mumbai",   "SQL,Python"),
    (16, "Riya",  24, None,  103, "HR",           "Gurgaon",  "Excel,HR"),
    (17, "Aman",  27, 62000, 101, "Engineering",  "Delhi",    "Python,Spark"),
    (18, "Dev",   32, 88000, 104, "Finance",      "Mumbai",   "SQL,Excel"),
    (19, "Ananya",25, 68000, 102, "Analytics",    "Noida",    "Python,Spark"),
    (20, "Kabir", 28, 76000, 103, "HR",           "Gurgaon",  "Excel,Python"),
    (21, "Kabir", 28, 76000, 1010, "HR",           "Gurgaon",  "Excel,Python"),
]

columns = [
    "employee_id",
    "name",
    "age",
    "salary",
    "department_id",
    "department",
    "location",
    "skills"
]

df = spark.createDataFrame(data, columns)