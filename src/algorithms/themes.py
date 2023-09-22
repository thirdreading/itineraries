"""
themes.py
"""
import logging
import os

import dask
import pandas as pd

import src.functions.objects


class Themes:
    """
    Themes
    """

    def __init__(self, data: pd.DataFrame, publication_type: pd.DataFrame, theme: pd.DataFrame, storage: str):
        """

        :param data:
        :param publication_type: The publication type reference dataframe
        :param theme: The theme reference dataframe
        :param storage:
        """

        # ...
        self.__instances = data.copy().merge(publication_type, how='inner', on='publication_id')
        self.__theme = theme
        self.__storage = storage

        # fields
        self.__fields = {'epoch': 'x', 'publication_series': 'name', 'synopsis': 'description',
                         'publication_type': 'publication_type'}

        # logging
        logging.basicConfig(level=logging.INFO,
                            format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    def exc(self):
        pass
