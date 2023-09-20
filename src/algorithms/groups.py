import pandas as pd


class Groups:

    def __init__(self, publication_type: pd.DataFrame, theme: pd.DataFrame):

        self.__publication_type = publication_type
        self.__theme = theme

    def __inner(self) -> pd.DataFrame:

        data = self.__publication_type.copy()
        data.rename(columns={'publication_id': 'id', 'publication_type': 'child_desc'}, inplace=True)

        data.loc[:, 'parent'] = 'publications'
        data.loc[:, 'parent_desc'] = 'Publications'

        return data

    def __outer(self, blob: pd.DataFrame):

        rename = {'theme_id': 'id', 'theme_name': 'child_desc',
                  'publication_id': 'parent', 'publication_type': 'parent_desc'}

        data = blob.copy()[['theme_id', 'publication_id']].drop_duplicates()
        data = data.merge(self.__theme, how='inner', on='theme_id')
        data = data.merge(self.__publication_type, how='inner', on='publication_type')
        data.rename(columns=rename, inplace=True)

        return data

    def exc(self, data: pd.DataFrame):
        pass
