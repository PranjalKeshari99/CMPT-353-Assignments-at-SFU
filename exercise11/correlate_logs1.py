import sys
from pyspark.sql import SparkSession, functions, types, Row
import re
import math

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
        hostname = m.group(1)
        num_bytes = m.group(2)
        return Row(hostname=hostname, num_bytes=num_bytes)
    else:
        return None

def not_none(row):
    """
    Is this None? Hint: .filter() with it.
    """
    return row is not None


def create_row_rdd(in_directory):
    log_lines = spark.sparkContext.textFile(in_directory)
    # TODO: return an RDD of Row() objects
    rows = log_lines.map(line_to_row)
    rows = rows.filter(not_none)
    return rows


def main(in_directory):
    logs = spark.createDataFrame(create_row_rdd(in_directory)).cache()

    #Group by the host name and count the number of requests by each host
    logs_names = logs.groupby('hostname').count().alias('num_requests')
    logs_bytes = logs.groupby('hostname').agg(functions.sum('num_bytes').alias('total_bytes'))
    logs = logs_bytes.join(logs_names, 'hostname', 'inner')
    
    # TODO: calculate r.
    #x_i is the count requests for each host
    #y_i is the number of requested bytes for each host
    logs = logs.select(
        logs['count'].alias('xi'),
        logs['total_bytes'].alias('yi'),
        (logs['count']*logs['total_bytes']).alias('xiyi'),
        (logs['count']*logs['count']).alias('xi^2'),
        (logs['total_bytes']*logs['total_bytes']).alias('yi^2')
        )

    logs.show()

    n = logs.count()
    sum_xi = logs.agg(functions.sum('xi')).first()[0]
    sum_yi = logs.agg(functions.sum('yi')).first()[0]
    sum_xi_2 = logs.agg(functions.sum('xi^2')).first()[0]
    sum_yi_2 = logs.agg(functions.sum('yi^2')).first()[0]
    sum_xiyi = logs.agg(functions.sum('xiyi')).first()[0]

    r = ((n*sum_xiyi)-(sum_xi*sum_yi))/((math.sqrt((n*sum_xi_2)-(sum_xi**2)))*(math.sqrt((n*sum_yi_2)-(sum_xi**2))))
    
    print("r = %g\nr^2 = %g" % (r, r**2))


if __name__=='__main__':
    in_directory = sys.argv[1]
    #in_directory = 'nasa-logs-1/*.gz'
    main(in_directory)
