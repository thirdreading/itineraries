"""
disaggregates.py
"""
import logging

import dask
import pandas as pd


class Disaggregates:
    """
    Disaggregates
    """

    def __init__(self, data: pd.DataFrame, publication_type: pd.DataFrame, theme: pd.DataFrame, storage: str):
        """

        :param data:
        :param publication_type: The publication type reference dataframe
        :param theme: The theme reference dataframe
        :param storage:
        """

        self.__data = data
        self.__publication_type = publication_type
        self.__theme = theme
        self.__storage = storage

        # fields
        self.__fields = {'epoch': 'x', 'publication_series': 'name', 'synopsis': 'description'}

        # logging
        logging.basicConfig(level=logging.INFO,
                            format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    @dask.delayed
    def __by_publication(self, code: str):
        """

        :param code:
        :return:
        """

        frame: pd.DataFrame = self.__data.copy().loc[self.__data['publication_id'] == code, self.__fields.keys()]
        frame.rename(columns=self.__fields, inplace=True)

    def exc(self):
        """

        :return:
        """

        self.__logger.info('%s', self.__data.info())
        codes = self.__data['publication_id'].unique()

        computation = []
        for code in codes:
            pass
