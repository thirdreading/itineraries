"""
disaggregates.py
"""
import logging
import pandas as pd

import dask


class Disaggregates:
    """
    Disaggregates
    """

    def __init__(self, publication_type: pd.DataFrame, theme: pd.DataFrame, storage: str):
        """

        :param publication_type: The publication type reference dataframe
        :param theme: The theme reference dataframe
        :param storage:
        """

        self.__publication_type = publication_type
        self.__theme = theme
        self.__storage = storage

        # logging
        logging.basicConfig(level=logging.INFO,
                            format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    def __epoch(self, blob: pd.DataFrame):

        length = len('YYYY-mm-dd')
        left = blob.copy()['publication_date'].str.slice(start=(length - 2), stop=length)
        self.__logger.info(left)

    def exc(self, data: pd.DataFrame):
        """

        :param data:
        :return:
        """

        self.__logger.info('%s', data.info())
        self.__logger.info(data.head())

        self.__epoch(blob=data)
