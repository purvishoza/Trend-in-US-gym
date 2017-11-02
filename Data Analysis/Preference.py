#What type of Gyms rated Higher based on attributes.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("BUzDataFram").getOrCreate()

file2 = spark.read.json(r"C:\Users\kusha\Downloads\BigData\Python\Demo\JSON\Business.json")

file2.createOrReplaceTempView('B')
spark.catalog.cacheTable('B')
BuzDF = spark.table('B')


l = ['BikeParking: True', 'ByAppointmentOnly: False', 'BusinessAcceptsCreditCards: True', 'AcceptsInsurance: True', 'WheelchairAccessible: True'  ]

### Only open Gym businesses  in AZ
b = BuzDF.filter(BuzDF.is_open == '1').select(explode(BuzDF.attributes), BuzDF.stars, BuzDF.state, BuzDF.business_id, BuzDF.name, BuzDF.city, BuzDF.is_open, BuzDF.review_count, BuzDF.latitude, BuzDF.longitude)
b.groupby(b.col).count().show(b.count(), False)
b2 = b.filter(b.col.isin(l)).filter(b.state == 'AZ').select(b.col, b.name, b.stars).orderBy(b.stars.desc())
b2.show(b2.count(), False)
b3 = b2.select(b2.name, b2.stars).distinct()
b4 = b3.groupby(b3.stars).count()
b4.coalesce(1).write.csv('c.csv')

l = b.filter(b.col.isin(l)).filter(b.state == 'AZ').filter(b.city == 'Phoenix').filter(b.stars > 3.5).select(b.col, b.name, b.stars, b.review_count, b.latitude, b.longitude).orderBy(b.stars.desc())
l.show(l.count(), False)
# l = b.filter(b.col.isin(l)).filter(b.state == 'AZ').filter(b.city == 'Phoenix').filter(b.stars < 4).select(b.col, b.name, b.stars, b.review_count, b.latitude, b.longitude).orderBy(b.stars.desc())
#l.show(l.count(), False)
# l.coalesce(1).write.csv('m.csv')


### only open Gym businesses in NV
b = BuzDF.filter(BuzDF.is_open == '1').select(explode(BuzDF.attributes), BuzDF.stars, BuzDF.state, BuzDF.city, BuzDF.business_id, BuzDF.name, BuzDF.is_open, BuzDF.review_count, BuzDF.latitude, BuzDF.longitude)
#b.groupby(b.col).count().show(b.count(), False)
b2 = b.filter(b.col.isin(l)).filter(b.state == 'NV').select(b.col, b.name, b.stars, b.city).orderBy(b.stars.desc())
# b2.show(b2.count(), False)
# b3 = b2.select(b2.name, b2.stars).distinct()
# b4 = b3.groupby(b3.stars).count()
# b4.coalesce(1).write.csv('c.csv')


l = b.filter(b.col.isin(l)).filter(b.state == 'NV').filter(b.city == 'Las Vegas').filter(b.stars > 3.5).select(b.col, b.name, b.stars, b.review_count, b.latitude, b.longitude).orderBy(b.stars.desc())
# l.coalesce(1).write.csv('m.csv')
# l.show(l.count(), False)
# l = b.filter(b.col.isin(l)).filter(b.state == 'NV').filter(b.city == 'Las Vegas').filter(b.stars < 4).select(b.col, b.name, b.stars, b.review_count, b.latitude, b.longitude).orderBy(b.stars.desc())
# l.coalesce(1).write.csv('m.csv')


spark.stop()







