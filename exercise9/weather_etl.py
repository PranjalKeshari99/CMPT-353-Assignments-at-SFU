import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('weather ETL').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),
])


def main(in_directory, out_directory):
    weather = spark.read.csv(in_directory, schema=observation_schema)
    
    # TODO: finish here.
    #Keep only the records we care about:
    #1.field qflag (quality flag) is null; (Hint)
    #2.the station starts with 'CA'; (Hint option 1; Hint option 2)
    #3.the observation is 'TMAX'.
    cleaned_data = weather.filter(weather.qflag.isNull())
    cleaned_data = cleaned_data.filter(cleaned_data.station.startswith('CA'))
    cleaned_data = cleaned_data.filter(cleaned_data['observation'] == 'TMAX')

    #Divide the temperature by 10 so it's actually in °C.
    cleaned_data = cleaned_data.withColumn('tmax', cleaned_data['value'] / 10)

    #Keep only the columns station, date, and tmax (which is the value after dividing by 10).
    cleaned_data = cleaned_data.select(cleaned_data['station'], cleaned_data['date'], cleaned_data['tmax'])
    cleaned_data.show()
    
    cleaned_data.write.json(out_directory, compression='gzip', mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
