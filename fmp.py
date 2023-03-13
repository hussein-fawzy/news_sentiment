import math
import os
import pandas as pd
import printcontrol as pc
import re
import requests
import time

from datastorage import DataStorage
from dateoperations import DEFAULT_DATE_FORMAT, DEFAULT_DATETIME_FORMAT
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class FMP:
    _SYMBOL_PLACEHOLDER = "###"
    _API_KEY_PLACEHOLDER = "@@@"
    _PAGE_PLACEHOLDER = "$$$"
    _BASE_URL = 'https://financialmodelingprep.com/api/v{v}/'
    
    _NEWS_BASE_URL = _BASE_URL.format(v = 3) + 'stock_news'
    _NEWS_URL = _NEWS_BASE_URL + '?tickers=' + _SYMBOL_PLACEHOLDER + '&apikey=' + _API_KEY_PLACEHOLDER + '&page=' + _PAGE_PLACEHOLDER
    
    BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "fmp")
    _NEWS_DATA_DIR = os.path.join(BASE_DIR, "news")                             #directory containing historical news files for each downloaded symbol
    _SOCIAL_SENTIMENT_DATA_DIR = os.path.join(BASE_DIR, "social_sentiment")     #directory containing historical social sentiment files for each downloaded symbol

    #set api limit
    #Starter plan allows 300 calls / minute
    #check https://financialmodelingprep.com/developer/docs/pricing
    CALL_DELAY = 60 / 300 #delay between api calls in seconds


    def __init__(self, symbol):
        #remove symbol extension
        dot_index = symbol.find(".")

        if dot_index > -1:
            symbol = symbol[:dot_index]
            
        self.symbol = symbol

        #prepare data storages (index is date strings and columns are financial entry names)
        self.news_ds = DataStorage(symbol, FMP._NEWS_DATA_DIR)                              #data storage with news
        self.social_sentiment_ds = DataStorage(symbol, FMP._SOCIAL_SENTIMENT_DATA_DIR)      #data storage with social sentiments

        self._load_data()

    def _load_data(self):
        """
        load symbol data from the local file system
        """

        #load the data storages
        #if a ds is not loaded, it means that this is the first time to use this ds, so add a date column and set it as index
        if not self.news_ds.read_data(index_col = "date"):
            self.news_ds.add_column("date")
            self.news_ds.df.set_index("date", inplace = True)

            #add columns for sentiment
            self.news_ds.add_column("sentiment")                #-1: negative, 0: neutral, 1: positive
            self.news_ds.add_column("sentiment_probability")    #confidence in sentiment (range: [0:1])

        if not self.social_sentiment_ds.read_data(index_col = "date"):
            self.social_sentiment_ds.add_column("date")
            self.social_sentiment_ds.df.set_index("date", inplace = True)


    def read_news(self, verbose = False):
        """
        read historical news and store them in news datastorage

        verbose: if True, progress messages will be printed
        """

        #NOTE: method name should not be changed as it is used in news.download_news_datasets()
        
        url = FMP._NEWS_URL.replace(FMP._SYMBOL_PLACEHOLDER, self.symbol)

        if len(self.news_ds.df) > 0:
            cross_check = {"series": self.news_ds.df["title"], "dict_key": "title"}
        else:
            cross_check = None

        entries = FMP._read_entries(url, cross_check = cross_check, verbose = verbose)

        #remove non-required columns
        columns_to_remove = ['symbol', 'image']
        entries.drop(columns = columns_to_remove, inplace = True)

        #add entries to ds
        FMP._add_entries_to_ds(self.news_ds, entries)

        #sort and save the downloaded data
        DataStorage.sort_ds_dates(self.news_ds, date_format = DEFAULT_DATE_FORMAT)
        self.news_ds.save_data(save_index = True)

    def add_sentiment_to_news(self):
        """
        set sentiment parameters for the downloaded news rows
        sentiments are calculated using VADER
        references:
        - https://blog.quantinsti.com/vader-sentiment/
        - https://scribe.rip/m/global-identity?redirectUrl=https%3A%2F%2Ftowardsdatascience.com%2Fsentimental-analysis-using-vader-a3415fef7664
        """

        if len(self.news_ds.df) == 0:
            return

        #create SentimentIntensityAnalyzer object
        sid = SentimentIntensityAnalyzer()

        #extract and update rows that have no sentiment
        partial_df = self.news_ds.df[self.news_ds.df["sentiment"].isnull()]

        for idx, row in partial_df.iterrows():
            #obtain sentiment
            #if the text is NaN, an exception will be thrown. try to analyze the title in this case
            try:
                sentiment_dict = sid.polarity_scores(row["text"])
            except: 
                try:
                    sentiment_dict = sid.polarity_scores(row["title"])
                except:
                    self.news_ds.df.loc[idx, "sentiment"] = 0
                    self.news_ds.df.loc[idx, "sentiment_probability"] = 0

                    continue

            negative = sentiment_dict['neg']
            neutral = sentiment_dict['neu']
            positive = sentiment_dict['pos']
            compound = sentiment_dict['compound']

            if compound >= 0.05:
                overall_sentiment = 1
                sentiment_probability = positive
            elif compound <= -0.05:
                overall_sentiment = -1
                sentiment_probability = negative
            else:
                overall_sentiment = 0
                sentiment_probability = neutral

            #add sentiment to df
            self.news_ds.df.loc[idx, "sentiment"] = overall_sentiment
            self.news_ds.df.loc[idx, "sentiment_probability"] = sentiment_probability

        #save the dataset
        self.news_ds.save_data(save_index = True)

    def aggregate_news_sentiment(self, freq = "1D"):
        """
        aggregate news sentiment to daily values

        freq: sentiment aggregation period
        return: pandas series with date index (sorted from earlier to newer) and aggregated sentiment as values
        """

        def aggregate_sample_sentiment(sample):
            """
            calculate the aggregate sentiment for a given sample
            the aggregation is the sum of the sentiment * sentiment_probability for each row in the sample

            sample: values dataframe with "sentiment" and "sentiment_probability" columns to calculate the aggregation for
            return: float|sample sentiment aggregation
            """

            if len(sample) == 0:
                return 0

            sample_aggregation = (sample["sentiment"] * sample["sentiment_probability"]).sum()
            return sample_aggregation

        #convert index of original dataframe to datetime index to allow date range masking
        self.news_ds.df.index = pd.to_datetime(self.news_ds.df.index, format = DEFAULT_DATETIME_FORMAT)

        aggregatation = self.news_ds.df.groupby(pd.Grouper(freq = freq)).apply(aggregate_sample_sentiment)

        #convert index to strings
        self.news_ds.df.index = self.news_ds.df.index.strftime(DEFAULT_DATETIME_FORMAT)

        return aggregatation


    def read_social_sentiment(self, verbose = False):
        """
        read historical social sentiment and store it in social_sentiment datastorage

        verbose: if True, progress messages will be printed
        """

        url = FMP._SOCIAL_SENTIMENT_URL.replace(FMP._SYMBOL_PLACEHOLDER, self.symbol)

        if len(self.social_sentiment_ds.df) > 0:
            cross_check = {"series": self.social_sentiment_ds.df.index.to_series(), "dict_key": "date"}
        else:
            cross_check = None

        entries = FMP._read_entries(url, cross_check = cross_check, verbose = verbose)

        #remove non-required columns
        columns_to_remove = ['symbol']
        entries.drop(columns = columns_to_remove, inplace = True)

        #add entries to ds
        FMP._add_entries_to_ds(self.social_sentiment_ds, entries)

        #sort and save the downloaded data
        DataStorage.sort_ds_dates(self.social_sentiment_ds, date_format = DEFAULT_DATETIME_FORMAT) #dates obtained from the social sentiment api follow DEFAULT_DATETIME_FORMAT
        self.social_sentiment_ds.save_data(save_index = True)


    @staticmethod
    def get_news_data_dir():
        """
        return the data directory used to store and retrieve news

        return: full data directory for the news files
        """

        return FMP._NEWS_DATA_DIR

    @staticmethod
    def _read_entries(url, cross_check = None, verbose = False):
        """
        read financial or news entries from an endpoint

        url: endpoint url to load the data from with the FMP._API_KEY_PLACEHOLDER
        cross_check: dictionary used to decide when to stop reading further pages
            - should contain the following keys:
                - "series": pandas series used to check what values are already downloaded
                - "dict_key": key in the read response dictionary to cross-check in the given series
            - only used when the url supplied is for the news api
            - if None, all pages will be read
        verbose: if True, progress messages will be printed
        return: pandas dataframe of entry names as columns and date strings as indices
        """

        #read endpoint response
        import config

        url = url.replace(FMP._API_KEY_PLACEHOLDER, config.fmp_api_key)

        #check if the page read is a news or social sentiment page (multiple pages should be read in these cases)
        is_news = url.startswith(FMP._NEWS_BASE_URL)


        #news are spread of multiple pages. read them accordingly
        curr_page = -1
        response_list = []

        if verbose: pc_base_line = pc.last_line

        while True:
            loop_start_time = time.time()

            curr_page += 1
            curr_url = url.replace(FMP._PAGE_PLACEHOLDER, str(curr_page))

            if verbose: pc.reprint(pc_base_line + " >> Reading page {}...".format(curr_page + 1))
            
            response = requests.get(curr_url)
            curr_response_list = response.json() #list of dictionariess

            if len(curr_response_list) == 0:
                break

            response_list += response.json()

            #if the latest entry in the current page is available in the cross-check series, there is no need to read the next pages as they are already available
            if cross_check is not None and (cross_check["series"].eq(response_list[-1][cross_check["dict_key"]])).any():
                break

            #delay after each call (except for the last call)
            loop_end_time = time.time()
            loop_total_time = loop_end_time - loop_start_time
            loop_delay = FMP.CALL_DELAY - loop_total_time #subtract time that is already consumed by the loop from the required delay

            if loop_delay > 0:
                time.sleep(loop_delay)

        if len(response_list) == 0:
            raise ValueError("Could not obtain data. Symbol may not be available.")

        #convert response to pandas dataframe
        response_df = pd.json_normalize(response_list)

        #rename date column from the news endpoint to follow project standards (fmp name is publishedDate)
        if is_news:
            response_df.rename(columns = {"publishedDate": "date"}, inplace = True)

        #Note: it is not required to update the date format of fmp as it already matches the required date format of DEFAULT_DATE_FORMAT
        #DEFAULT_DATE_FORMAT or DEFAULT_DATETIME_FORMAT (from common.dateoperations) are used for financials or news, respectively.

        #set date as index
        try:
            response_df.set_index("date", inplace = True)
        except KeyError:
            raise KeyError("Incorrect data format obtained (No \"date\" column). First row: {}".format(response_df.iloc[0, :].values))

        #convert column names from camelCase to snake_case (lower-case underscore-separated words)
        camel_case_to_snake_case = lambda name: re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
        response_df.rename(columns = camel_case_to_snake_case, inplace = True)

        return response_df

    @staticmethod
    def _add_entries_to_ds(ds, entries):
        """
        add financial or news entries to a given data storage object

        ds: data storage object to add entries to
        entries: dataframe of new entries to add to the given ds
        """

        #find rows and columns in entries that are not in ds
        missing_rows = list(set(entries.index.tolist()) - set(ds.df.index.tolist()))
        missing_columns = list(set(entries.columns) - set(ds.df.columns))

        #add missing rows and columns to ds
        for row in missing_rows:
            ds.df.loc[row, :] = math.nan

        for column in missing_columns:
            ds.df.loc[:, column] = math.nan

        #update ds
        ds.df.update(entries)
