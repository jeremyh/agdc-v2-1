from datacube import Datacube


def high_level_write():
    with Datacube() as dc:
        # Create product if needed
        # Measurements are derived from xarray
        # Uses current default storage driver
        dc.save('ls7_ndvi_albers', xarray, crs=None)


def write_albers_product_low_level():
    with Datacube() as dc:
        my_xarray = None
        
        product = dc.create_product(
            'ls7_nbar_albers',
            create_measurements_for_xarray(my_xarray),

            # Optional
            description='Landsat 7 Surface Reflectance NBAR 25 metre, 100km tile, '
                        'Australian Albers Equal Area projection (EPSG:3577)',

            fields=dict(
                crs='epsg:3577',
                platform='LANDSAT_8',
                instrument='ETM',
                format='NetCDF CF',
            ),

            storage_params=dict(
            )
        )
        
        dataset = dc.create_dataset(
            product,
            my_xarray,
            fields={},
            source_datasets={}
        )

        location = dc.write_data(dataset, my_xarray)


def prepare_script(image_path):
    """
    Prepare scripts generate a metadata document on disk for a given image.

    They would be optional with the above api, but are useful for some operational cubes 
    (who want to store a copy of metadata on disk, such as for archival) 
    """

    # - Load from file path into xarray
    my_xarray = None
    # - Extra extra metadata fields as needed
    fields = {}

    with Datacube() as dc:
        dataset = dc.create_dataset(
            'ls7_nbar_scene',
            my_xarray,
            fields=fields,
            source_datasets={}
        )

        write_yaml(dataset, filename)


def create_measurements_for_xarray(my_array):
    pass
