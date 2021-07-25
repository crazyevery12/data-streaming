import os
import re
from operator import add

from pyspark import SparkConf, SparkContext, SQLContext


datafile_json = 'Software_5.json'

def get_keyval(row):

    # get the text from the row entry
    reviewText = row.reviewText

    #remove unwanted chars
    reviewText=re.sub("\\W"," ",reviewText)

    # lower case text and split by space to get the words
    words = reviewText.lower ().split (" ")

    # for each word, send back a count of 1
    return [[w, 1] for w in words]

def get_counts(df):
    # just for the heck of it, show 2 results without truncating the fields
    df.show (2, False)

    # for each text entry, get it into tokens and assign a count of 1
    # we need to use flat map because we are going from 1 entry to many
    mapped_rdd = df.rdd.flatMap (lambda row: get_keyval (row))

    # for each identical token (i.e. key) add the counts
    # this gets the counts of each word
    counts_rdd = mapped_rdd.reduceByKey (add)

    # get the final output into a list
    word_count = counts_rdd.collect ()

    # print the counts
    for e in word_count:
        print (e)

def process_json(abspath, sparkcontext):

    # Create an sql context so that we can query data files in sql like syntax
    sqlContext = SQLContext (sparkcontext)

    # read the json data file and select only the field labeled as "text"
    # this returns a spark data frame
    df = sqlContext.read.json (os.path.join (
        abspath, datafile_json)).select ("reviewText")

    # use the data frame to get counts of the text field
    get_counts(df)
    
if __name__ == "__main__":

    # absolute path to this file
    abspath = os.path.abspath(os.path.dirname(__file__))
    # Create a spark configuration with 20 threads.
    # This code will run locally on master
    conf = (SparkConf ()
            . setMaster("local[20]")
            . setAppName("sample app for reading files")
            . set("spark.executor.memory", "2g"))

    sc = SparkContext(conf=conf)

    # process the json data file
    process_json (abspath, sc)

