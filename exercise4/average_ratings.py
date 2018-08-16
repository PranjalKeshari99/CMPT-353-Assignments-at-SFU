
# Created by Philip Leblanc June 7, 2018 for CMPT 353 exercise 4 at Simon Fraser University
# Takes files in the command line to find average movie ratings
# This is an exercise in data manipulation designed to test data manipulation data in python
# Required input files: movie_list.txt & movie_ratings.csv
# Output : output.csv (run in the command line like: "python3 average_ratings.py movie_list.txt movie_ratings.csv output.csv") 

import numpy as np
import pandas as pd
import sys # for command line arguments
import difflib

movie_list = sys.argv[1]
movie_ratings = sys.argv[2]
output_file = sys.argv[3]
movies = open(movie_list).readlines()
movies = list(map(lambda s: s.strip(), movies)) #strip whitespace by default with the strip func map object needs to be converted back to list


df = pd.DataFrame(movies, columns=['title'])
#df['movies'] = df['movies'].replace('\n','', regex= True)

#find the average movie ratings from the 
ratings = pd.read_csv(movie_ratings)


def get_movie_title(word):
    matches = difflib.get_close_matches(word, movies, n=500)
    return matches


ratings['title'] = ratings['title'].apply(lambda title: get_movie_title(title))
ratings['title'] = ratings.title.apply(''.join) #to get ride of brackets around the movie title
ratings = ratings[ratings.title != ''] #remove empty strings
ratings = ratings.reset_index(drop=True) #drops the old index


ratings = ratings.groupby('title', 0).mean().reset_index() #reset_index to the title

output_df = df.merge(ratings, on='title')
output_df['rating'] = output_df['rating'].round(2)


output_df.to_csv(output_file)

