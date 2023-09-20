import logging
import pandas as pd


class Groups:

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

    def __inner(self) -> pd.DataFrame:

        data = self.__publication_type.copy()
        data.rename(columns={'publication_id': 'id', 'publication_type': 'child_desc'}, inplace=True)

        data.loc[:, 'parent'] = 'publications'
        data.loc[:, 'parent_desc'] = 'Publications'

        return data

    def __outer(self, blob: pd.DataFrame) -> pd.DataFrame:

        rename = {'theme_id': 'id', 'theme_name': 'child_desc',
                  'publication_id': 'parent', 'publication_type': 'parent_desc'}

        data = blob.copy()[['theme_id', 'publication_id']].drop_duplicates()
        data = data.merge(self.__theme, how='inner', on='theme_id')
        data = data.merge(self.__publication_type, how='inner', on='publication_id')
        data.rename(columns=rename, inplace=True)

        return data

    def exc(self, data: pd.DataFrame):

        inner = self.__inner()
        self.__logger.info(inner)

        outer = self.__outer(blob=data)
        self.__logger.info(outer)
