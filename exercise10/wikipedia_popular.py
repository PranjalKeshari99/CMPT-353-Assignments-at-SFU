import sys
from pyspark.sql import SparkSession, functions, types
import re

spark = SparkSession.builder.appName('wikipedia popular').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

schema = types.StructType([ # commented-out fields won't be read
    types.StructField('language', types.StringType(), False),
    types.StructField('page_name', types.StringType(), False),
    types.StructField('views', types.LongType(), False),
    types.StructField('bytes', types.LongType(), False),
])


def getdate(pathname):
	result = re.search("([0-9]{8}\-[0-9]{2})", pathname)
	return result.group(1)


def main(in_directory, out_directory):
    wikipedia = spark.read.csv(in_directory, schema=schema, sep=' ').withColumn('filename', functions.input_file_name())

    # TODO: find the the most-viewed page each hour
    cleaned_data = wikipedia.filter(wikipedia['language'] == 'en')
    cleaned_data = cleaned_data.filter(cleaned_data['page_name'] != 'Main_Page')
    cleaned_data = cleaned_data.filter(cleaned_data.page_name.startswith('Special:') == False)


    path_to_hour = functions.udf(lambda path: getdate(path), returnType=types.StringType())

    cleaned_data = cleaned_data.withColumn('date', path_to_hour(cleaned_data.filename))
    cleaned_data = cleaned_data.drop('language', 'bytes', 'filename')


    groups = cleaned_data.groupBy('date')
    most_viewed = groups.agg(functions.max(cleaned_data['views']).alias('views')) #most views by date
    most_viewed.cache()

    cond = ['views', 'date']
    data_joined = most_viewed.join(cleaned_data, cond)

    output = data_joined.sort('date', 'page_name')

    output.show()

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
