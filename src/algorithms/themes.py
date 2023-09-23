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

    @dask.delayed
    def __excerpt(self, theme_id: str) -> pd.DataFrame:
        """

        :param theme_id:
        :return:
        """

        frame: pd.DataFrame = self.__instances.copy().loc[
            self.__instances['theme_id'] == theme_id, self.__fields.keys()]
        frame.rename(columns=self.__fields, inplace=True)
        frame.loc[:, 'label'] = frame['name'].array

        return frame

    @dask.delayed
    def __node(self, blob: pd.DataFrame, theme_id: str, theme_name: str) -> dict:
        """

        :param blob:
        :param theme_id:
        :param theme_name:
        :return:
        """

        return {'name': theme_id,
                'desc': theme_name,
                'data': blob.to_dict(orient='tight')}

    def exc(self):
        """

        :return:
        """

        codes = self.__theme.merge(
            self.__instances[['theme_id']].drop_duplicates(), how='inner', on='theme_id')

        computations = []
        for theme_id, theme_name in zip(codes['theme_id'], codes['theme_name']):
            excerpt = self.__excerpt(theme_id=theme_id)
            node = self.__node(blob=excerpt, theme_id=theme_id, theme_name=theme_name)
            computations.append(node)

        dask.visualize(computations, filename='dag', format='pdf')
        items = dask.compute(computations, scheduler='threads')[0]
        message = src.functions.objects.Objects().write(
            nodes=items, path=os.path.join(self.__storage, 'themes.json'))

        self.__logger.info(message)
