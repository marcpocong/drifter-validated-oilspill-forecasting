import xarray as xr

try:
    ds = xr.open_dataset('data/forcing/cmems_curr.nc')
    print("Variables:", list(ds.data_vars))
    print("\nVariable Details:")
    for var in ds.data_vars:
        print(f"{var}: {ds[var].attrs.get('long_name', 'No long_name')} ({ds[var].attrs.get('units', 'No units')})")
        print(f"  Standard name: {ds[var].attrs.get('standard_name', 'None')}")
except Exception as e:
    print(f"Error: {e}")
