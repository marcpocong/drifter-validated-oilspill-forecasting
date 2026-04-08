import xarray as xr
import os

def fix_metadata(filepath):
    print(f"🔧 Fixing metadata for: {filepath}")
    temp_path = filepath + "_fixed.nc"

    if not os.path.exists(filepath):
        print("   ❌ File not found.")
        return

    try:
        # 1. READ
        with xr.open_dataset(filepath) as ds:
            ds.load()
            data = ds.copy(deep=True)
        
        # 2. IDENTIFY VARIABLES
        x_var = 'x_wind' if 'x_wind' in data else ('u10' if 'u10' in data else None)
        y_var = 'y_wind' if 'y_wind' in data else ('v10' if 'v10' in data else None)

        if x_var and y_var:
            # 3. FIX METADATA (Standard Names)
            data[x_var].attrs['standard_name'] = 'eastward_wind'
            data[y_var].attrs['standard_name'] = 'northward_wind'
            data[x_var].attrs['units'] = 'm/s'
            data[y_var].attrs['units'] = 'm/s'
            
            # 4. FIX ENCODING (The NCEP Fix)
            # Remove conflicting 'missing_value' so xarray uses '_FillValue' (NaN) only
            for var in [x_var, y_var]:
                if 'missing_value' in data[var].encoding:
                    del data[var].encoding['missing_value']
                if '_FillValue' in data[var].encoding:
                    del data[var].encoding['_FillValue'] # Let xarray generate a fresh one

            print(f"   ✅ Cleaned encoding & tagged {x_var}/{y_var}")
            
            # 5. WRITE & SWAP
            data.to_netcdf(temp_path)
            data.close()
            
            if os.path.exists(filepath):
                os.remove(filepath)
            os.rename(temp_path, filepath)
            print("   🔄 File swapped successfully.")
            
        else:
            print(f"   ⚠️ Could not find wind variables.")

    except Exception as e:
        print(f"   ❌ Error: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)