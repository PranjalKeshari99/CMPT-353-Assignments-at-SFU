import sys
import numpy as np
import pandas as pd
import scipy.stats as stats


OUTPUT_TEMPLATE = (
    '"Did more/less users use the search feature?" p-value: {more_users_p:.3g}\n'
    '"Did users search more/less?" p-value: {more_searches_p:.3g}\n'
    '"Did more/less instructors use the search feature?" p-value: {more_instr_p:.3g}\n'
    '"Did instructors search more/less?" p-value: {more_instr_searches_p:.3g}'
)


def main():
    input_file = sys.argv[1]
    searches = pd.read_json(input_file, orient='records', lines=True) #even is old interface odd is new
    # ...
    even_uid = searches[searches['uid']%2 == 0] #old interface
    odd_uid = searches[searches['uid']%2 == 1] #new interface
    #1.Did more users use the search feature? Did a different fraction of users have search count > 0?
    #2.Did users search more often? Is the number of searches per user different?

    num_even_notzero = len(even_uid[even_uid['search_count'] > 0].index) #number of users with search count > 0 for old interface
    num_odd_notzero = len(odd_uid[odd_uid['search_count'] > 0].index) #number of users with search count > 0 for new interface
    num_even_zero = len(even_uid[even_uid['search_count'] == 0].index)
    num_odd_zero = len(odd_uid[odd_uid['search_count'] == 0].index)

    #chi-squared works on categories. Count the number in each category and fill in the table with the counts.
    contingency = [[num_even_notzero, num_even_zero], [num_odd_notzero, num_odd_zero]]
    chi, p, dof, ex = stats.chi2_contingency(contingency)

    even_instructor = even_uid[even_uid['is_instructor'] == True]
    odd_instructor = odd_uid[odd_uid['is_instructor'] == True] 

    instructor_even_notzero = len(even_instructor[even_instructor['search_count'] > 0].index) #number of intructors with search count > 0 for old interface
    instructor_odd_notzero = len(odd_instructor[odd_instructor['search_count'] > 0].index) #number of instructors with search count > 0 for new interface
    instructor_even_zero = len(even_instructor[even_instructor['search_count'] == 0].index)
    instructor_odd_zero = len(odd_instructor[odd_instructor['search_count'] == 0].index)

    contingency = [[instructor_even_notzero, instructor_even_zero], [instructor_odd_notzero, instructor_odd_zero]]
    chi2, p2, dof2, ex2 = stats.chi2_contingency(contingency)

    # Output
    print(OUTPUT_TEMPLATE.format(
        more_users_p=p,
        more_searches_p=stats.mannwhitneyu(odd_uid['search_count'],even_uid['search_count']).pvalue,
        more_instr_p=p2,
        more_instr_searches_p=stats.mannwhitneyu(odd_instructor['search_count'],even_instructor['search_count']).pvalue,
    ))


if __name__ == '__main__':
    main()