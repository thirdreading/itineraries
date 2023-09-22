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

        # ...
        self.__instances = data.copy().merge(theme, how='inner', on='theme_id')
        self.__publication_type = publication_type
        self.__storage = storage
        
        # fields
        self.__fields = {'epoch': 'x', 'publication_series': 'name', 'synopsis': 'description', 'theme': 'theme'}

        # logging
        logging.basicConfig(level=logging.INFO,
                            format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    @dask.delayed
    def __by_publication(self, code: str) -> pd.DataFrame:
        """

        :param code:
        :return:
        """

        frame: pd.DataFrame = self.__instances.copy().loc[self.__instances['publication_id'] == code, self.__fields.keys()]
        frame.rename(columns=self.__fields, inplace=True)
        
        return frame
        
    def __node(self, blob: pd.DataFrame, code: str):
        pass
        
    def exc(self):
        """

        :return:
        """

        self.__logger.info('%s', self.__instances.info())
        codes = self.__instances['publication_id'].unique()

        computation = []
        for code in codes:
            pass
