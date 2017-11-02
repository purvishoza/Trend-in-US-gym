from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("BUzDataFram").getOrCreate()

file = spark.read.json(r"C:\Users\kusha\Downloads\BigData\Python\Demo\JSON\Review.json")

file2 = spark.read.json(r"C:\Users\kusha\Downloads\BigData\Project\yelp\yelp_academic_dataset_user.json")

joindf = file.join(file2, file.user_id == file2.user_id, 'inner').select(file2["user_id"], file2["name"], file2["review_count"], file2["yelping_since"], file2["friends"], file2["useful"], file2["funny"], file2["cool"], file2["fans"], file2["elite"], file2["average_stars"], file2["compliment_hot"], file2["compliment_more"], file2["compliment_profile"], file2["compliment_cute"],
                                                                         file2["compliment_list"], file2["compliment_note"], file2["compliment_plain"], file2["compliment_cool"], file2["compliment_funny"], file2["compliment_writer"], file2["compliment_photos"], file2["type"]).distinct()
joindf.coalesce(1).write.json('c.json')

spark.stop()