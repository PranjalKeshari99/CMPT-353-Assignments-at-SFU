import sys
import string, re
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('word count').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+




def main(in_directory, out_directory):
    # 1. Read lines from the files with spark.read.text.
    lines = spark.read.text(in_directory).cache()

    # 2. Split the lines into words with the regular expression below. 
    # Use the split and explode functions. 
    # Normalize all of the strings to lower-case (so “word” and “Word” are not counted separately.)
    wordbreak = r'[%s\s]+' % (re.escape(string.punctuation),)  # regex that matches spaces and/or punctuation
    words = lines.select(functions.split(lines.value, wordbreak).alias('value')).cache()
    words = words.select(functions.explode(words.value).alias('value'))
    words = words.select(functions.lower(words.value).alias('word'))

    # 3. Count the number of times each word occurs.
    word_count = words.groupBy(words.word).count().cache()

    # 4. Sort by decreasing count (i.e. frequent words first) and alphabetically if there's a tie.
    word_count = word_count.sort(word_count['count'].desc()).cache()

    # 5. Notice that there are likely empty strings being counted: remove them from the output. 
    # (They come from spaces at the start/end of lines in the original input.)
    word_count = word_count.filter(word_count['word'] != '')

    # 6. Write results as CSV files with the word in the first column, 
    # and count in the second (uncompressed: they aren't big enough to worry about).
    word_count.write.csv(out_directory)




if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)