"""
publications.py
"""
import logging
import os

import dask
import pandas as pd

import src.functions.objects


class Publications:
    """
    Publications
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
        self.__fields = {'epoch': 'x', 'publication_series': 'name', 'synopsis': 'description',
                         'theme_name': 'theme_name'}

        # logging
        logging.basicConfig(level=logging.INFO,
                            format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    @dask.delayed
    def __excerpt(self, publication_id: str) -> pd.DataFrame:
        """

        :param publication_id:
        :return:
        """

        frame: pd.DataFrame = self.__instances.copy().loc[
            self.__instances['publication_id'] == publication_id, self.__fields.keys()]
        frame.rename(columns=self.__fields, inplace=True)
        
        return frame

    @dask.delayed
    def __node(self, blob: pd.DataFrame, publication_id: str, publication_type: str) -> dict:

        return {'name': publication_id,
                'desc': publication_type,
                'data': blob.to_dict(orient='records')}

    def exc(self):
        """

        :return:
        """

        self.__logger.info('%s', self.__instances.info())

        codes = self.__publication_type.merge(
            self.__instances[['publication_id']].drop_duplicates(), how='inner', on='publication_id')

        computations = []
        for publication_id, publication_type in zip(codes['publication_id'], codes['publication_type']):
            excerpt = self.__excerpt(publication_id=publication_id)
            node = self.__node(blob=excerpt, publication_id=publication_id, publication_type=publication_type)
            computations.append(node)

        dask.visualize(computations, filename='dag', format='pdf')
        items = dask.compute(computations, scheduler='threads')[0]
        message = src.functions.objects.Objects().write(
            nodes=items, path=os.path.join(self.__storage, 'publications.json'))

        self.__logger.info(message)
