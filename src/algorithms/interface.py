"""
interface.py
"""
import logging
import os

import src.functions.directories
import src.functions.streams
import src.algorithms.groups


class Interface:
    """
    Interface
    """

    def __init__(self):
        """
        Constructor
        """

        # directories
        self.__source = os.path.join(os.getcwd(), 'data')
        self.__storage = os.path.join(os.getcwd(), 'warehouse', 'data')
        self.__set_up()

        # data
        self.__streams = src.functions.streams.Streams()
        self.__publication_type = self.__streams.read(uri=os.path.join(self.__source, 'publication_type.csv'))
        self.__theme = self.__streams.read(uri=os.path.join(self.__source, 'theme.csv'))
        self.__schedule = self.__streams.read(uri=os.path.join(self.__source, 'restructured', 'schedule.csv'))

    def __set_up(self):
        """

        :return: None
        """

        directories = src.functions.directories.Directories()
        directories.cleanup(path=self.__storage)
        directories.create(path=self.__storage)

    def __groups(self):

        src.algorithms.groups.Groups(
            publication_type=self.__publication_type, theme=self.__theme, storage=self.__storage).exc(
            data=self.__schedule)

    def __disaggregates(self):
        pass

    def exc(self):

        self.__groups()
