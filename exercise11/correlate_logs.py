import sys
from pyspark.sql import SparkSession, functions, types, Row
import re

spark = SparkSession.builder.appName('correlate logs').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

line_re = re.compile(r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$")


def line_to_row(line):
    """
    Take a logfile line and return a Row object with hostname and bytes transferred. Return None if regex doesn't match.
    """
    m = line_re.match(line)
    if m:
        # TODO
        host = m.group(1)
        nbytes = m.group(2)
        return Row(hostname=host, num_bytes=nbytes)
    else:
        return None

def not_none(row):
    """
    Is this None? Hint: .filter() with it.
    """
    return row is not None


def create_row_rdd(in_directory):
    line = spark.sparkContext.textFile(in_directory)
    # TODO: return an RDD of Row() objects
    rows = line.map(line_to_row)
    rows = rows.filter(not_none)
    return rows


def main(in_directory):
    # 1. Get the data out of the files into a DataFrame where you have the hostname 
    # and number of bytes for each request. Do this using an RDD operation: see hints
    logs = spark.createDataFrame(create_row_rdd(in_directory)).cache

    # TODO: calculate r.
    # 2. Group by hostname; get the number of requests and sum of bytes transferred, 
    # to form a data point 
    log_host = logs.groupby('hostname').count().alias('num_requests')
    log_bytes = logs.groupby('hostname').agg(functions.sum('num_bytes').alias('total_bytes'))
    log = log_bytes.join(log_host, 'hostname')

    # 3. Produce six values. Add these to get the six sums.
    logs = log.select(
        log['count'].alias('xi'), log['total_bytes'].alias('yi'),
        (log['count']*log['total_bytes']).alias('xiyi'),
        (log['count']*log['count']).alias('xi^2'),
        (log['total_bytes']*log['total_bytes']).alias('yi^2'))

    n = logs.count()
    sum_xi = logs.agg(functions.sum('xi')).first()[0]
    sum_yi = logs.agg(functions.sum('yi')).first()[0]
    sum_xi_2 = logs.agg(functions.sum('xi^2')).first()[0]
    sum_yi_2 = logs.agg(functions.sum('yi^2')).first()[0]
    sum_xiyi = logs.agg(functions.sum('xiyi')).first()[0]

    # 4. Calculate the final value of .
    r = ((n*sum_xiyi)-(sum_xi*sum_yi))/((math.sqrt((n*sum_xi_2)-(sum_xi**2)))*(math.sqrt((n*sum_yi_2)-(sum_xi**2))))


    print("r = %g\nr^2 = %g" % (r, r**2))


if __name__=='__main__':
    in_directory = sys.argv[1]
    main(in_directory)
