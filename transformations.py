from pyproj import Transformer
import pandas as pd

def get_sample_coordinates():
    """Return a list of sample North American city coordinates."""
    return [
        (-73.553785, 45.508722),  # Montreal, Canada
        (-118.243683, 34.052235), # Los Angeles, USA
        (-77.036873, 38.907192),  # Washington, D.C., USA
        (-123.120738, 49.282729), # Vancouver, Canada
        (-80.191790, 25.761680)   # Miami, USA
    ]

def create_transformers():
    """
    Create transformation objects for comparing EPSG:5070 (NAD83) and 
    NLCD custom projection (WGS84) which share the same Albers Equal Area 
    parameters but differ in datum.
    
    Returns:
        tuple: (transformer_nad83, transformer_wgs84) transformation objects
    """
    # Standard EPSG:5070 (NAD83 / Conus Albers)
    # This can be done with EPSG code since it's a standard CRS
    transformer_nad83 = Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=True)
    
    # NLCD Custom projection (WGS84-based Albers Equal Area)
    # Using from_proj since this is a custom CRS not in the EPSG database
    transformer_wgs84 = Transformer.from_proj(
        proj_from="EPSG:4326",  # Can use EPSG code for source
        proj_to=("+proj=aea "
                "+lat_0=23 "
                "+lon_0=-96 "
                "+lat_1=29.5 "
                "+lat_2=45.5 "
                "+x_0=0 "
                "+y_0=0 "
                "+datum=WGS84 "
                "+units=m "
                "+no_defs"),
        always_xy=True
    )
    
    return transformer_nad83, transformer_wgs84

def transform_coordinates(coordinates, transformer_nad83, transformer_wgs84):
    """Transform coordinates and calculate differences between projections."""
    results = []
    for lon, lat in coordinates:
        # Transform to EPSG:5070 (NAD83 / Conus Albers)
        nad83_x, nad83_y = transformer_nad83.transform(lon, lat)
        
        # Transform to custom WGS84-based Albers Equal Area projection
        wgs84_x, wgs84_y = transformer_wgs84.transform(lon, lat)
        
        # Calculate differences in projected coordinates
        x_diff = wgs84_x - nad83_x
        y_diff = wgs84_y - nad83_y
        
        results.append({
            "Longitude": lon,
            "Latitude": lat,
            "NAD83_X": nad83_x,
            "NAD83_Y": nad83_y,
            "WGS84_X": wgs84_x,
            "WGS84_Y": wgs84_y,
            "X_Diff (m)": x_diff,
            "Y_Diff (m)": y_diff
        })
    
    return results

def display_results(results):
    """Display the transformation results in a DataFrame."""
    df_results = pd.DataFrame(results)
    print("Comparison of EPSG:5070 (NAD83 / Conus Albers) and Custom WGS84-Based Albers Equal Area Projection:")
    print(df_results)
    return df_results

def main():
    """Main function to run the coordinate transformations."""
    coordinates = get_sample_coordinates()
    transformer_nad83, transformer_wgs84 = create_transformers()
    results = transform_coordinates(coordinates, transformer_nad83, transformer_wgs84)
    display_results(results)

if __name__ == "__main__":
    main()
