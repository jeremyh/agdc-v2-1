"""Generic index module. It contains a concrete index for managing
initial queries when we don't know the driver associated with the
dataset yet, and an abstract index extension class to be implemented
by each driver's index in order to properly add driver-specific index
data to the dataset document at creation.
"""
from __future__ import absolute_import

import logging

from abc import ABCMeta, abstractmethod
from six import add_metaclass

from datacube.config import LocalConfig
import datacube.index._api as base_index
from datacube.index.postgres import PostgresDb


@add_metaclass(ABCMeta)
class IndexExtension(object):
    """Abstract base class for index extensions specific to a driver.
    """

    @abstractmethod
    def add_specifics(self, dataset):
        """Extend the dataset doc with driver specific index data.

        The dataset is modified in place.

        :param :cls:`datacube.model.Dataset` dataset: The dataset to
          add driver-specific indexing data to.
        """
        pass


class Index(base_index.Index, IndexExtension):
    """Generic driver.

    This driver uses the legacy index to pull basic dataset data from
    the DB, but then call back into the driver manager to let the
    appropriate driver-specific index add specific data to the
    document if needed.

    This index should not be used when storing data. Instead, use
    driver-specific sub-classes of this index.
    """

    def __init__(self, driver_manager, db):
        """Initialise the generic index.

        :param PostgresDb db:
        :param args: Optional positional arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all potential
          indexes.
        :param kargs: Optional keyword arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all potential
          indexes.

        """
        self.logger = logging.getLogger(self.__class__.__name__)
        super(Index, self).__init__(driver_manager, db)

    def add_specifics(self, dataset):
        """This method does not make sense for the generic driver.

        This index should not be used when storing data. Instead, use
          driver-specific sub-classes of this index.
        """
        raise NotImplementedError('This generic driver can only be used to retrieve basic data')
