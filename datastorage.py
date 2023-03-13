"""
pandas-based data storage and manipulation system
stores and reads data to and from a csv file
"""

import os
import pandas as pd


class DataStorage:
    EXT = ".csv"   #storage file extension


    def __init__(self, file_name, base_dir, columns = None, column_types = None):
        """
        initialize a DataStorage object
        initializes the data storage name (file_name without "_" and capitalized first letter)
        initializes a data frame for holding the data

        file_name: file name of the data storage without the directory and without extensions
        base_dir: directory to store to or read from
        columns: columns to use in the data storage
            - if not None, a header-less data storage file is saved (not required to save column names to the file if they are forced when the data storage is initialized)
            - if None and no dataframe is loaded (with read_data), integer column names will be created when using add_rows
            - if None and a dataframe is loaded (with read_data) from a file that has no column names, the first row from the file will be considered as the column names
        column_types: data type for each column. used when reading the storage file
        """

        self._name = " ".join([word.capitalize() for word in file_name.split("_")]) #capitalize first letters and remove "_"
        self.file_path = os.path.join(base_dir, file_name + DataStorage.EXT)

        self.columns = columns
        self.column_types = column_types

        self.df = pd.DataFrame(columns = columns)

    def get_name(self):
        """
        get the name of the data storage (file_name with no "_" and capitalized first letters)

        return: name (string)
        """

        return self._name


    def read_data(self, header = 0, index_col = False):
        """
        read the db file and add the required columns

        header: row number to use for column names. this parameter is ignored if self.columns is not None
        index_col: column number (integer) or name (string) to use as the index
        return: True if file exists and opened, False otherwise
        """

        if not os.path.isfile(self.file_path):
            return False

        if self.columns is not None:
            self.df = pd.read_csv(self.file_path, names = self.columns, dtype = self.column_types, index_col = index_col)
        else:
            self.df = pd.read_csv(self.file_path, header = header, index_col = index_col)

        return True

    def save_data(self, save_index = False, file_path = None):
        """
        save the data frame to a file without the column names row or index column

        save_index: if True, index labels are saved
        file_path: complete file path (directory + file name + extension) to use instead of self.file_path
        """

        if file_path is None:
            file_path = self.file_path

        #create directory if it does not exist
        storage_dir = os.path.dirname(file_path)
        if not os.path.exists(storage_dir):
            os.makedirs(storage_dir)

        #if self.column is not None, no column names are saved since the function assumes that column names are hard-coded
        if self.columns is not None:
            self.df.to_csv(file_path, index = save_index, header = False)
        else:
            self.df.to_csv(file_path, index = save_index, header = True)


    def set_dataframe(self, df, columns = None, column_types = None):
        """
        set the dataframe for this datastorage object

        df: dataframe to use
        columns: columns to use in the data storage
        column_types: data type for each column. used when reading the storage file
        """

        self.columns = columns
        self.column_types = column_types

        self.df = df

    def index_to_column(self, inplace = True):
        """
        convert the dataframe index to a column
        does not modify self.columns

        inplace: if False, a new dataframe is created (and returned) without modifying the original one
        return: None if inplace = True, a new dataframe with the index converted to a column if inplace = False
        """

        df = self.df.reset_index(inplace = inplace)
        return df

    def column_to_index(self, column_name, inplace = True):
        """
        convert a column from the dataframe to be the index
        method can be used even if self.columns is None
        old index is removed

        column_name: column name to set as the index
        inplace: if False, a new dataframe is created (and returned) without modifying the original one
        return: None if inplace = True, a new dataframe with the index converted to a column if inplace = False
        """

        df = self.df.set_index(column_name, inplace = inplace)
        return df


    def add_column(self, column_name, default_value = None):
        """
        add a column to the data frame
        does not modify self.columns

        column_name: a name to use for the new column
        default_value: value to use in the added column for all rows available
        """

        self.df[column_name] = default_value

    def add_rows(self, rows, indices = None):
        """
        append rows to the data frame

        rows: list of lists of the values for each column of the data frame
        indices: list of indices for the rows to be added. set to None to use a range index
        """

        #if this instance is initialized with no columns and no file is loaded, then self.df will have no columns, so set columns to None when creating df_new
        if len(self.df.columns) == 0:
            columns = None
        else:
            columns = self.df.columns

        if indices is None:
            df_new = pd.DataFrame(rows, columns = columns)
            self.df = self.df.append(df_new, ignore_index = True)
        else:
            df_new = pd.DataFrame(rows, columns = columns, index = indices)
            self.df = self.df.append(df_new)

    def update_rows(self, condition_column, condition, condition_value, columns_to_update, update_value):
        """
        find and update rows where a certain column's value is greater than, less than, or equal to the given condition_value

        condition_column: column to search for the condition_value inside (filter column)
        condition: comparison operator (gt: greater than, lt: less than, eq: equal to, neq: not equal to)
        value_value: required value to filter rows by
        columns_to_update: array of columns. the rows to be updated will be updated in these columns
        update_value: value to be written in columns_to_update
        """

        #array of True where the row meet the required condition in the required column and False otherwise
        if condition == "gt":
            bool_filter = self.df[condition_column] > condition_value
        elif condition == "lt":
            bool_filter = self.df[condition_column] < condition_value
        elif condition == "eq":
            if condition_value is not None:
                bool_filter = self.df[condition_column] == condition_value
            else:
                bool_filter = self.df[condition_column].isnull()
        elif condition == "neq":
            if condition_value is not None:
                bool_filter = self.df[condition_column] != condition_value
            else:
                bool_filter = self.df[condition_column].notnull()

        self.df.loc[bool_filter, columns_to_update] = update_value


    def get_rows_by_condition(self, column, condition, value):
        """
        find rows where a certain column's value is greater than, less than, or equal to the given value

        column: column to search for the value inside
        condition: comparison operator (gt: greater than, lt: less than, eq: equal to, neq: not equal to
        value: required value to filter rows by

        return: data frame with rows that meet the required condition in the required column
        """

        #array of True where the row meet the required condition in the required column and False otherwise
        if condition == "gt":
            bool_filter = self.df[column] > value
        elif condition == "lt":
            bool_filter = self.df[column] < value
        elif condition == "eq":
            if value is not None:
                bool_filter = self.df[column] == value
            else:
                bool_filter = self.df[column].isnull()
        elif condition == "neq":
            if value is not None:
                bool_filter = self.df[column] != value
            else:
                bool_filter = self.df[column].notnull()
        else:
            return

        return self.df[bool_filter]

    def remove_rows_by_condition(self, column, condition, value):
        """
        find and remove rows where a certain column's value is greater than, less than, or equal to the given value

        column: column to search for the value inside
        condition: comparison operator (gt: greater than, lt: less than, eq: equal to, neq: not equal to)
        value: required value to filter rows by
        """

        #array of True where the row meet the required condition in the required column and False otherwise
        if condition == "gt":
            bool_filter = self.df[column] > value
        elif condition == "lt":
            bool_filter = self.df[column] < value
        elif condition == "eq":
            if value is not None:
                bool_filter = self.df[column] == value
            else:
                bool_filter = self.df[column].isnull()
        elif condition == "neq":
            if value is not None:
                bool_filter = self.df[column] != value
            else:
                bool_filter = self.df[column].notnull()
        else:
            return

        self.df = self.df.drop(self.df[bool_filter].index)


    @staticmethod
    def sort_ds_dates(ds, date_format):
        """
        sort data storage by dates index in a reverse order (newest data first)
        columns are also sorted by name

        ds: data storage to sort
        """

        #convert index to datetime index
        ds.df.index = pd.to_datetime(ds.df.index, format = date_format)

        #sort by index
        ds.df.sort_index(ascending = False, inplace = True)

        #convert index back to strings
        ds.df.index = ds.df.index.strftime(date_format)

        #sort columns
        ds.df = ds.df.reindex(sorted(ds.df.columns), axis = 1)

    @staticmethod
    def intersect_on_column(dataframes, column_name):
        """
        for the given dataframes, the intersection between them on the required column is found then each of the given dataframe is filtered on this intersection
        column_name must be available in all given dataframes
        example usage: given two - or more - dataframes where one of the columns is showing the date (and named "date") and intersecting on this column, the result will be 
            a new list of dataframes with date rows that are available in all dataframes. if a date is available on only one of the dataframes, it will not be available 
            in the resulting dataframe since its not common to all dataframes

        dataframes: list of pandas dataframes to find the intersection between
        column_name: str|column name to find the intersection with
        return: list of pandas dataframe with the intersected rows on the given column_name
        """

        #find the intersected dataframe
        intersection_df = pd.merge_ordered(dataframes[0], dataframes[1], how = "inner", on = column_name)

        for i in range(2, len(dataframes)):
            intersection_df = pd.merge_ordered(intersection_df, dataframes[i], how = "inner", on = column_name)

            #remove all columns - except the column being merged on - to improve performance of sub-sequent merge operations
            #without this, performance degrades after each merge as each merge adds extra columns
            columns_to_remove = intersection_df.columns.to_list()
            columns_to_remove.remove(column_name)
            intersection_df.drop(columns = columns_to_remove, inplace = True)

        #filter the given dataframes with the intersection_df column_name
        intersection_dataframes = [df[df[column_name].isin(intersection_df[column_name])] for df in dataframes]

        return intersection_dataframes
