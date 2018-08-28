import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit relative scores').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

schema = types.StructType([ # commented-out fields won't be read
    #types.StructField('archived', types.BooleanType(), False),
    types.StructField('author', types.StringType(), False),
    #types.StructField('author_flair_css_class', types.StringType(), False),
    #types.StructField('author_flair_text', types.StringType(), False),
    #types.StructField('body', types.StringType(), False),
    #types.StructField('controversiality', types.LongType(), False),
    #types.StructField('created_utc', types.StringType(), False),
    #types.StructField('distinguished', types.StringType(), False),
    #types.StructField('downs', types.LongType(), False),
    #types.StructField('edited', types.StringType(), False),
    #types.StructField('gilded', types.LongType(), False),
    #types.StructField('id', types.StringType(), False),
    #types.StructField('link_id', types.StringType(), False),
    #types.StructField('name', types.StringType(), False),
    #types.StructField('parent_id', types.StringType(), True),
    #types.StructField('retrieved_on', types.LongType(), False),
    types.StructField('score', types.LongType(), False),
    #types.StructField('score_hidden', types.BooleanType(), False),
    types.StructField('subreddit', types.StringType(), False),
    #types.StructField('subreddit_id', types.StringType(), False),
    #types.StructField('ups', types.LongType(), False),
])


def main(in_directory, out_directory):
    comments = spark.read.json(in_directory, schema=schema).cache()

    # TODO
    # 1.Calculate the average score for each subreddit, as before
    averages = comments.groupby('subreddit').agg(functions.avg('score').alias('avg_score')).cache()
    
    # 2.Exclude any subreddits with average score ≤0.
    averages = averages.filter(averages.avg_score > 0)
    
    # 3.Join the average score to the collection of all comments. Divide to get the relative score.
    averages = averages.join(functions.broadcast(comments), 'subreddit', 'inner')
    #averages = averages.join(comments, 'subreddit')
    averages = averages.withColumn('relative_score', averages.score/averages.avg_score)
    
    # 4.Determine the max relative score for each subreddit.
    averages = averages.groupby('subreddit').agg(functions.max('relative_score').alias('relative_score'))

    # 5.Join again to the best comment on each subreddit: we need this step to get the author.
    max_score = comments.groupby('subreddit').agg(functions.max('score').alias('score')).cache()
    max_score = max_score.join(functions.broadacst(comments), ['subreddit', 'score'], 'inner')
    #max_score = max_score.join(comments, ['subreddit', 'score'])
    best_author = max_score.join(functions.broadcast(averages), 'subreddit', 'inner').drop('score')
    #best_author = max_score.join(averages, 'subreddit').drop('score')

    best_author.write.json(out_directory, mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
