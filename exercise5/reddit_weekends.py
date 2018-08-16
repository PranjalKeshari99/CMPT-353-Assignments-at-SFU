import pandas as pd
import numpy as np
import sys
import gzip
import datetime
import scipy.stats as stats
import matplotlib

OUTPUT_TEMPLATE = (
    "Initial (invalid) T-test p-value: {initial_ttest_p:.3g}\n"
    "Original data normality p-values: {initial_weekday_normality_p:.3g} {initial_weekend_normality_p:.3g}\n"
    "Original data equal-variance p-value: {initial_levene_p:.3g}\n"
    "Transformed data normality p-values: {transformed_weekday_normality_p:.3g} {transformed_weekend_normality_p:.3g}\n"
    "Transformed data equal-variance p-value: {transformed_levene_p:.3g}\n"
    "Weekly data normality p-values: {weekly_weekday_normality_p:.3g} {weekly_weekend_normality_p:.3g}\n"
    "Weekly data equal-variance p-value: {weekly_levene_p:.3g}\n"
    "Weekly T-test p-value: {weekly_ttest_p:.3g}\n"
    "Mann–Whitney U-test p-value: {utest_p:.3g}"
)


def main():
    reddit_counts = sys.argv[1]

    counts_fh = gzip.open(reddit_counts, 'rt', encoding='utf-8')
    counts = pd.read_json(counts_fh, lines=True)

    counts = counts[counts['date'] >= datetime.date(2012, 1, 1)]
    counts = counts[counts['date'] <= datetime.date(2013,12,31)]
    counts = counts[counts['subreddit'] == 'canada']
    counts = counts.reset_index(drop=True)

    weekdays = counts[(counts['date'].map(lambda x: datetime.date.weekday(x)) != 5) & (counts['date'].map(lambda x: datetime.date.weekday(x)) != 6)]
    weekdays = weekdays.reset_index(drop=True)
    weekdays_mean = weekdays['comment_count'].mean()

    weekends = counts[(counts['date'].map(lambda x: datetime.date.weekday(x)) == 5) | (counts['date'].map(lambda x: datetime.date.weekday(x)) == 6)]
    weekends = weekends.reset_index(drop=True)
    weekends_mean = weekends['comment_count'].mean()

    #T-test p-value less than 0.05 reject the null hypothesis
    initial_ttest_p = stats.ttest_ind(weekdays['comment_count'], weekends['comment_count'], ).pvalue
    levene_pvalue = stats.levene(weekdays['comment_count'], weekends['comment_count'],).pvalue
    weekdays_normaltest_pvalue = stats.normaltest(weekdays['comment_count']).pvalue
    weekends_normaltest_pvalue = stats.normaltest(weekends['comment_count']).pvalue

    #fix 1 to transform the data to a different scale
    #log
    weekdays_log = weekdays['comment_count'].apply(np.log)
    weekends_log = weekends['comment_count'].apply(np.log)
    weekdays_log_ntest_pvalue = stats.normaltest(weekdays_log).pvalue
    weekends_log_ntest_pvalue = stats.normaltest(weekends_log).pvalue
    weekends_log_levene_pvalue = stats.levene(weekends_log, weekdays_log).pvalue

    #exp
    weekdays_exp = weekdays['comment_count'].apply(np.exp)
    weekends_exp = weekends['comment_count'].apply(np.exp)
    weekdays_exp_ntest_pvalue = stats.normaltest(weekdays_exp).pvalue
    weekends_exp_ntest_pvalue = stats.normaltest(weekends_exp).pvalue
    weekends_exp_levene_pvalue = stats.levene(weekends_exp, weekdays_exp).pvalue
    
    #sqrt
    weekdays_sqrt = weekdays['comment_count'].apply(np.sqrt)
    weekends_sqrt = weekends['comment_count'].apply(np.sqrt)
    weekdays_sqrt_ntest_pvalue = stats.normaltest(weekdays_sqrt).pvalue
    weekends_sqrt_ntest_pvalue = stats.normaltest(weekends_sqrt).pvalue
    weekends_sqrt_levene_pvalue = stats.levene(weekends_sqrt, weekdays_sqrt).pvalue

    #sqr
    weekdays_sqr = weekdays['comment_count']**2
    weekends_sqr = weekends['comment_count']**2
    weekdays_sqr_ntest_pvalue = stats.normaltest(weekdays_sqr).pvalue
    weekends_sqr_ntest_pvalue = stats.normaltest(weekends_sqr).pvalue
    weekends_sqr_levene_pvalue = stats.levene(weekends_sqr, weekdays_sqr).pvalue

    #fix 2 central limit theorem

    #combine all weekdays and weekend days from each year/week pair and take the mean of their (non-transformed) counts.
    date = weekdays['date'].apply(datetime.date.isocalendar).apply(pd.Series) #date with year, month and dat in seperate columns
    date = date[[0, 1]] #only interested in year and month
    date.columns=['Year', 'Week'] #give an appropriate name to columns
    weekdays = pd.concat([weekdays, date], axis=1)
    #same idea for weekends
    date1 = weekends['date'].apply(datetime.date.isocalendar).apply(pd.Series) #date with year, month and dat in seperate columns
    date1 = date1[[0, 1]] #only interested in year and month
    date1.columns=['Year', 'Week'] #give an appropriate name to columns
    weekends = pd.concat([weekends, date1], axis=1)

    weekdays_grouped = weekdays.groupby(['Year', 'Week']).aggregate('mean').reset_index()
    weekends_grouped = weekends.groupby(['Year', 'Week']).aggregate('mean').reset_index()

    weekdays_grouped_ntest_pval = stats.normaltest(weekdays_grouped['comment_count']).pvalue
    weekends_grouped_ntest_pval = stats.normaltest(weekends_grouped['comment_count']).pvalue
    weekends_grouped_levene_pval = stats.levene(weekdays_grouped['comment_count'],weekends_grouped['comment_count']).pvalue

    weekdays_grouped_ttest_pval = stats.ttest_ind(weekdays_grouped['comment_count'], weekends_grouped['comment_count']).pvalue

    utest_p = stats.mannwhitneyu(weekends['comment_count'], weekdays['comment_count']).pvalue

    print(OUTPUT_TEMPLATE.format(
        initial_ttest_p=initial_ttest_p,
        initial_weekday_normality_p=weekdays_normaltest_pvalue,
        initial_weekend_normality_p=weekends_normaltest_pvalue,
        initial_levene_p=levene_pvalue,
        transformed_weekday_normality_p=weekdays_sqrt_ntest_pvalue,
        transformed_weekend_normality_p=weekends_sqrt_ntest_pvalue,
        transformed_levene_p=weekends_sqrt_levene_pvalue,
        weekly_weekday_normality_p=weekdays_grouped_ntest_pval,
        weekly_weekend_normality_p=weekends_grouped_ntest_pval,
        weekly_levene_p=weekends_grouped_levene_pval,
        weekly_ttest_p=weekdays_grouped_ttest_pval,
        utest_p=utest_p,
    ))


if __name__ == '__main__':
    main()