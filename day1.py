from pyspark.sql import SparkSession

# Start Spark
spark = SparkSession.builder \
    .appName("PySparkLearning") \
    .master("local[2]") \
    .getOrCreate()

# Our data
data = [
    (1, "Aman", 22, 50000),
    (2, "Rahul", 25, 70000),
    (3, "Priya", 24, 60000),
    (4, "Neha", 28, 90000)
]

new_data = [
    (5,"Rohit", 30, 85000),
    (6, "simran", 26, 65000)
]
data = data+new_data

# Column names
columns = ["id", "name", "age", "salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Display DataFrame
df.show()
# Display schema
df.printSchema()
df.select("name","age").show()
df.filter()
# Stop Spark
spark.stop()
