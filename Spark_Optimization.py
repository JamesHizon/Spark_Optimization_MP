"""
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month.
See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
"""

# Import PySpark and essential Spark SQL modules
# (Spark Session, functions - col, count and month)
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

# Obtain entry point into Spark through SparkSession object and create app
# called "Optimized I"
spark = SparkSession.builder.appName('Optimize I').getOrCreate()

# Obtain base path (current working directory) -> '/Users/jameshizon/PycharmProjects/MiniProjects'
# base_path = os.getcwd()

# For file path string manipulation, I may first have to add data into this folder.

# Project path
# project_path = ('/').join(base_path.split('/')[0:-3])

# Answers path
# answers_input_path = os.path.join(project_path, 'data/answers')

# Questions path
# questions_input_path = os.path.join(project_path, 'output/questions-transformed')

# Q/A Via Downloads Directory Path
# downloads_dir_path = "~/Downloads/Optimization/data/"
downloads_dir_path = "/Users/jameshizon/JupyterNB/data/"
questions_dd_path = downloads_dir_path + "questions/"
answers_dd_path = downloads_dir_path + "answers/"


# Create answers dataframe
answersDF = spark.read.option('path', answers_dd_path).load()

# Create questions dataframe
questionsDF = spark.read.option('path', questions_dd_path).load()

# These two dataframes will be joined in the following problem:

'''
Answers aggregation

Here we : get number of answers per question per month
'''

# # Apply Spark SQL query to obtain number of answers per question per month
# answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))  # Want to change groupBy -> reduceByKey.
#
# # Apply Spark SQL query to join previous table by question_id and select following columns from questionDF
# resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')  # We have a join -->
# # How can we change this? -> Maybe, we can try to join w/o shuffling.
#
# # Edit results displayed - order by question_id and month
# resultDF.orderBy('question_id', 'month').show()

# Initialize time object
start_time = time.time()

# Use withColumn(), groupBy() and agg()
answers_month = answersDF.withColumn('month', month('creation_date'))

answers_month = answers_month.groupBy('question_id', 'month').agg(count('*').alias('cnt'))

# Create resultDF by joining questionsDF on shared "question_id" column
resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

# Use orderBy() before showing result dataframe
resultDF.orderBy('question_id', 'month').show()

# Now, let us print out how many seconds it takes to obtain our desired result.

# print("-- %s seconds to process --" % (time.time() - start.time())
print("-- %s seconds to process --" % (time.time() - start_time))



'''
Task:

See the query plan of the previous result and rewrite the query to optimize it.
'''

# First, check explain plan:

resultDF.explain()

# Then, try to apply repartitioning to eliminate one shuffle.

# answers_month = answersDF.repartition('question_id').withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))
# # Q: Why repartition by question_id?
# resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')
#
# resultDF.orderBy('question_id', 'month').show()

start_time = time.time()

spark.conf.set("spark.sql.adaptive.enabled","true")

answers_month = answersDF.withColumn('month', month('creation_date'))

# Repartition by month
answers_month = answers_month.repartition(col("month"))

answers_month = answers_month.groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

print("-- %s seconds to process --" % (time.time() - start_time))

resultDF.explain()


