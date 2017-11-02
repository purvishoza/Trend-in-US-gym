from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("BUzDataFram").getOrCreate()

file = spark.read.json(r"C:\Users\kusha\Downloads\BigData\Python\Demo\JSON\Business.json")

file2 = spark.read.json(r"C:\Users\kusha\Downloads\BigData\Project\yelp\yelp_academic_dataset_review.json")

joindf = file.join(file2, file.business_id == file2.business_id, 'inner').select(file2["review_id"], file2["user_id"], file2["business_id"], file2["stars"], file2["date"], file2["text"], file2["useful"], file2["funny"], file2["cool"], file2["type"])
joindf.coalesce(1).write.json('c.json')

spark.stop()