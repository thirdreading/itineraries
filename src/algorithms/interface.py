import os

import pandas as pd

import src.functions.directories
import src.functions.streams


class Interface:

    def __init__(self):

        self.__source = os.path.join(os.getcwd(), 'data', 'schedule.csv')
        self.__storage = os.path.join(os.getcwd(), 'warehouse', 'data')
        self.__set_up()

    def __set_up(self):

        directories = src.functions.directories.Directories()
        directories.cleanup(path=self.__storage)
        directories.create(path=self.__storage)

    def __read(self) -> pd.DataFrame:

        data = src.functions.streams.Streams().read(uri=self.__source, header=0)

        return data

    def exc(self):
        pass
