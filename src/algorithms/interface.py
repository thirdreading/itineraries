"""
interface.py
"""
import logging
import os

import src.functions.directories
import src.functions.streams


class Interface:
    """
    Interface
    """

    def __init__(self):
        """
        Constructor
        """

        self.__source = os.path.join(os.getcwd(), 'data')
        self.__storage = os.path.join(os.getcwd(), 'warehouse', 'data')
        self.__set_up()

        self.__streams = src.functions.streams.Streams()

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

    def exc(self):
        pass
