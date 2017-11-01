from pprint import pprint

import gc

import pytest
import sys

from datacube.drivers.manager import DriverManager
from datacube.model import Dataset
from datacube.model import MetadataType

import objgraph


def test_crs_parse(indexed_ls5_scene_dataset_types):
    # type: (MetadataType) -> None
    product = indexed_ls5_scene_dataset_types[2]

    # Explicit CRS, should load fine.
    # Taken from LS8_OLI_NBAR_3577_-14_-11_20140601021126000000.nc
    d = Dataset(product, {
        "grid_spatial": {
            "projection": {
                "valid_data": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-1396453.986271351, -1100000.0], [-1400000.0, -1100000.0],
                         [-1400000.0, -1053643.4714392645], [-1392296.4215373022, -1054399.795365491],
                         [-1390986.9858215596, -1054531.808155645],
                         [-1390806.366757733, -1054585.3982497198],
                         [-1396453.986271351, -1100000.0]]
                    ]
                },
                "geo_ref_points": {
                    "ll": {"x": -1400000.0, "y": -1100000.0},
                    "lr": {"x": -1300000.0, "y": -1100000.0},
                    "ul": {"x": -1400000.0, "y": -1000000.0},
                    "ur": {"x": -1300000.0, "y": -1000000.0}},
                "spatial_reference": "EPSG:3577"
            }
        }

    }, local_uri=None)
    assert str(d.crs) == 'EPSG:3577'
    assert d.extent is not None

    # Valid datum/zone as seen on our LS5 scene, should infer crs.
    d = Dataset(product, {
        "grid_spatial": {
            "projection": {
                "zone": -51,
                "datum": "GDA94",
                "ellipsoid": "GRS80",
                "orientation": "NORTH_UP",
                "geo_ref_points": {
                    "ll": {"x": 537437.5, "y": 5900512.5},
                    "lr": {"x": 781687.5, "y": 5900512.5},
                    "ul": {"x": 537437.5, "y": 6117112.5},
                    "ur": {"x": 781687.5, "y": 6117112.5}
                },
                "map_projection": "UTM",
                "resampling_option": "CUBIC_CONVOLUTION"
            }
        }
    }, local_uri=None)
    assert str(d.crs) == 'EPSG:28351'
    assert d.extent is not None

    # No projection specified in the dataset
    d = Dataset(product, {}, local_uri=None)
    assert d.crs is None
    assert d.extent is None

    # Invalid datum/zone, can't infer
    d = Dataset(product, {
        "grid_spatial": {
            "projection": {
                "zone": -60,
                "datum": "GDA94",
                "ellipsoid": "GRS80",
                "orientation": "NORTH_UP",
                "geo_ref_points": {
                    "ll": {"x": 537437.5, "y": 5900512.5},
                    "lr": {"x": 781687.5, "y": 5900512.5},
                    "ul": {"x": 537437.5, "y": 6117112.5},
                    "ur": {"x": 781687.5, "y": 6117112.5}
                },
                "map_projection": "UTM",
                "resampling_option": "CUBIC_CONVOLUTION"
            }
        }
    }, local_uri=None)
    # Prints warning: Can't figure out projection: possibly invalid zone (-60) for datum ('GDA94')."
    # We still return None, rather than error, as they didn't specify a CRS explicitly
    assert d.crs is None


@pytest.mark.xfail(
    sys.version_info < (3, 2),
    reason="The usage of cachetools in py2 leaks references to old indexes. "
           "Fixed in py3 which uses the functools built-in"
)
def test_single_dm_instance(driver_manager, db):
    """
    Our driver manager should only be linked to one PostgresDb instance in memory.

    Subsequent drivers should share the same instance to avoid extra connections (and cache duplication)
    """

    # There's a circular reference in DriverManager structure, so old test instances aren't cleaned up by
    # the reference counter.
    gc.collect()

    # For all PostgresDb instances in memory (there may be others due to other pytests), count
    # how many are connected to our instance of DriverManager
    references = 0
    for pg_instance in objgraph.by_type('PostgresDb'):
        chain = objgraph.find_backref_chain(pg_instance, predicate=lambda o: isinstance(o, DriverManager))
        # If the referenced DriverManager is ours
        if chain[0] is driver_manager:
            pprint(chain)
            references += 1

    assert references == 1, "Our DriverManager should only reference one PG instance"
