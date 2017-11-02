from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("BUzDataFram").getOrCreate()

file = spark.read.json(r"C:\Users\kusha\Downloads\BigData\Project\yelp\yelp_academic_dataset_business.json")
file.createOrReplaceTempView('B')
spark.catalog.cacheTable('B')
BuzDF = spark.table('B')

StateList = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']

bDF = BuzDF.filter(BuzDF.state.isin(StateList)).filter(array_contains(BuzDF.categories, 'Gyms')).select(BuzDF['business_id'],BuzDF['name'],BuzDF['neighborhood'],BuzDF['address'],BuzDF['city'],BuzDF['state'],BuzDF['postal_code'],BuzDF['latitude'],BuzDF['longitude'],BuzDF['stars'],BuzDF['review_count'],BuzDF['is_open'],BuzDF['attributes'],BuzDF['categories'],BuzDF['hours'],BuzDF['type'])
#bDF = BuzDF.filter(BuzDF.state.isin(StateList)).filter(array_contains(BuzDF.categories, 'Gyms')).select('*')
bDF.show()
#bDF.coalesce(1).write.json('c.jason')

#st = bDF.groupby('state').count()
#st.coalesce(1).write.csv('c.csv')

spark.stop()
