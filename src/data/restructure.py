import os
import logging

import numpy as np
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

        # datestr
        self.__datestr_length = len('YYYY-mm-dd')

        # logging
        logging.basicConfig(level=logging.INFO,
                            format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    def __set_up(self):
        """

        :return: None
        """

        directories = src.functions.directories.Directories()
        directories.cleanup(path=self.__storage)
        directories.create(path=self.__storage)

    def __publication_type(self) -> pd.DataFrame:
        """

        :return: The publication type reference dataframe
        """

        return self.__streams.read(uri=os.path.join(self.__source, 'publication_type.csv'))

    def __theme(self) -> pd.DataFrame:
        """

        :return: The theme reference dataframe
        """

        return self.__streams.read(uri=os.path.join(self.__source, 'theme.csv'))

    def __schedule(self) -> pd.DataFrame:
        """

        :return: The schedule data, after renaming fields.
        """

        data = self.__streams.read(uri=os.path.join(self.__source, 'original', 'schedule.csv'), header=0)
        data.rename(mapper=str.lower, axis=1, inplace=True)
        data.columns = data.columns.str.replace(' ', '_')

        return data

    def __get_restructured_data(self) -> pd.DataFrame:
        """

        :return: A dataframe that includes the schedule alongside the code fields of publication & theme
        """

        original = self.__schedule()
        data = original.copy().merge(self.__publication_type(), how='inner', on='publication_type')
        data = data.copy().merge(self.__theme(), how='inner', on='theme_name')

        assert original.shape[0] == data.shape[0], 'Missing records due to unknown dimensions?'

        return data

    def __publication_state(self, blob: pd.DataFrame) -> pd.DataFrame:

        data = blob.copy()
        day = data['publication_date'].str.slice(start=(self.__datestr_length - 2),
                                                 stop=self.__datestr_length)
        data.loc[:, 'publication_day_released'] = np.where(day == '00', False, True)

        return data

    def exc(self):
        """

        :return:
        """

        # Get the restructured data, subsequently save the relevant fields
        data = self.__get_restructured_data()
        data.drop(columns=['publication_type', 'theme_name'], inplace=True)
        data = self.__publication_state(blob=data)

        self.__streams.write(blob=data, path=os.path.join(self.__storage, 'schedule.csv'))

        self.__logger.info('%s', data.info())
