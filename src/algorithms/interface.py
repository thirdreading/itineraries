import os
import logging

import pandas as pd

import src.functions.directories
import src.functions.streams


class Interface:

    def __init__(self):

        self.__source = os.path.join(os.getcwd(), 'data', 'schedule.csv')
        self.__storage = os.path.join(os.getcwd(), 'warehouse', 'data')
        self.__set_up()

        # logging
        logging.basicConfig(level=logging.INFO,
                            format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    def __set_up(self):

        directories = src.functions.directories.Directories()
        directories.cleanup(path=self.__storage)
        directories.create(path=self.__storage)

    def __read(self) -> pd.DataFrame:

        data = src.functions.streams.Streams().read(uri=self.__source, header=0)

        data.rename(mapper=str.lower, axis=1, inplace=True)
        data.columns = data.columns.str.replace(' ', '_')

        return data

    def exc(self):

        data = self.__read()
        self.__logger.info(data)

        self.__logger.info(data[['publication_type']].drop_duplicates())

        self.__logger.info(data[['theme']].drop_duplicates())

