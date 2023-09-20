import os
import logging

import pandas as pd

import src.functions.directories
import src.functions.streams


class Restructure:

    def __init__(self):
        """
        Constructor
        """

        self.__source = os.path.join(os.getcwd(), 'data')
        self.__storage = os.path.join(os.getcwd(), 'data', 'restructured')
        self.__set_up()

        # An instance for reading & writing data; <csv>.
        self.__streams = src.functions.streams.Streams()

        # logging
        logging.basicConfig(level=logging.INFO,
                            format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    def __set_up(self):

        directories = src.functions.directories.Directories()
        directories.cleanup(path=self.__storage)
        directories.create(path=self.__storage)

    def __publication_type(self) -> pd.DataFrame:

        return self.__streams.read(uri=os.path.join(self.__source, 'publication_type.csv'))

    def __theme(self) -> pd.DataFrame:

        return self.__streams.read(uri=os.path.join(self.__source, 'theme.csv'))

    def __schedule(self) -> pd.DataFrame:

        data = src.functions.streams.Streams().read(uri=os.path.join(self.__source, 'original', 'schedule.csv'), header=0)
        data.rename(mapper=str.lower, axis=1, inplace=True)
        data.columns = data.columns.str.replace(' ', '_')

        return data

    def exc(self):

        initial = self.__schedule()
        data = initial.copy().merge(self.__publication_type(), how='inner', on='publication_type')
        data = data.copy().merge(self.__theme(), how='inner', on='theme_name')

        assert initial.shape[0] == data.shape[0], 'Missing records due to unknown dimensions?'

        self.__logger.info('%s', initial.info())
        self.__logger.info('%s', data.info())
