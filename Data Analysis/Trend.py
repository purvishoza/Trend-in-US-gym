from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("BUzDataFram").getOrCreate()

file = spark.read.json(r"C:\Users\kusha\Downloads\BigData\Python\Demo\JSON\Checkin.json")
file2 = spark.read.json(r"C:\Users\kusha\Downloads\BigData\Python\Demo\JSON\Business.json")

file.createOrReplaceTempView('C')
spark.catalog.cacheTable('C')
CkinDF = spark.table('C')

file2.createOrReplaceTempView('B')
spark.catalog.cacheTable('B')
BuzDF = spark.table('B')

#n = BuzDF.groupby('name','state').count().filter('count > 1').orderBy(BuzDF.state)

def mapper(ls):
    str1 = []
    str1 = ls[0].split(':')
    max_val = str1

    for i in range(len(ls)):
        str1 = ls[i].split(':')
        if str1[1] > max_val[1]:
            max_val = str1

    return ":".join(max_val)

d = CkinDF.select(CkinDF.time, 'business_id').rdd.map(lambda x : (list(x[0]),x[1])).map(lambda x: (mapper(x[0]),x[1]))
df = spark.createDataFrame(d)
split_col = split(df['_1'],':')

df = df.withColumn('DayHour',split_col.getItem(0))
sp_split_col = split(df['DayHour'], '-')
df = df.withColumn('Day', sp_split_col.getItem(0))
df = df.withColumn('Hour', sp_split_col.getItem(1))
df = df.withColumn('Check_in', split_col.getItem(1))
df = df.withColumn('business_id', df['_2'])

df = df.drop('_1', '_2', 'DayHour')

joinDF = BuzDF.join(df, BuzDF.business_id == df.business_id, 'inner').select(BuzDF.business_id, BuzDF.name, BuzDF.city, BuzDF.state, df.Day, df.Hour, df.Check_in)
#joinDF.show(joinDF.count(), False)
weekdays = ['Mon','Tue','Wed','Thu','Fri']
weekends = ['Sat','Sun']
H1 = ['0','1','2','3','4','5','6','7','8','9']
H2 = ['10','11','12','13','14','15','16','17','18']
H3 = ['19','20','21','22','23']



# Which Day Businesses gets most number of checkins for all states and we can derive which day is prefered.
top = joinDF.filter(joinDF.Day.isin(weekdays)).filter(joinDF.Hour.isin(H2)).select(joinDF.state, joinDF.Day, joinDF.Hour, joinDF.Check_in)
windowSpec = Window.partitionBy(top['state'], top['Day']).orderBy(top['Check_in'].desc())
t = top.select('*', dense_rank().over(windowSpec).alias('rank')).orderBy(top['state'].asc(),top['Check_in'].desc()).filter(col('rank') <= 3).drop('rank')
t.show(t.count(),False)
# t.coalesce(1).write.csv('c.csv')


spark.stop()



