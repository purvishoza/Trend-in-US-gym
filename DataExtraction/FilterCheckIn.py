from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("BUzDataFram").getOrCreate()

file = spark.read.json(r"C:\Users\kusha\Downloads\BigData\Python\Demo\JSON\Business.json")
file2 = spark.read.json(r"C:\Users\kusha\Downloads\BigData\Project\yelp\yelp_academic_dataset_checkin.json")

joindf = file.join(file2, file.business_id == file2.business_id, 'inner').select(file2["time"], file2["business_id"], file2["type"])

joindf.coalesce(1).write.json('c.json')

spark.stop()
