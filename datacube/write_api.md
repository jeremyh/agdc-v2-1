



# High level api

Goal: convenient usage, such as in a notebook


    dc.save(array, name:str, storage_params={}, fields={})
    
- Uses low level api (below)
- Minimal product created implicitly if name doesn't exist

# Creating a product ahead of time

    product = dc.create_product()
    ...
    dc.save(array, product, storage_params={}, fields={})
    
    
# Fine-grained api

Finer-grained control, such as performing steps separately.


    product = dc.create_product(
                            name, 
                            measurements, 
                            md_type=None,
                            crs,
                            fields
    )
    
    dataset = dc.create_dataset(product, fields)
    
    location = dc.write(d, array, source_datasets={})
    
    
   
Then index these objects (possibly much later)
    
    index.products.add(product)
    index.datasets.add(dataset)
    
    # Possibly stored on dataset?
    index.datasets.add_location(location)
