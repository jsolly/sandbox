import psycopg
from dotenv import load_dotenv
import os
from contextlib import contextmanager

# Load environment variables from .env file
load_dotenv()

@contextmanager
def db_cursor():
    """
    Context manager for database operations. Handles connection and cursor management.
    Automatically closes cursor and connection when done.
    
    Yields:
        cursor: Database cursor object
    
    Usage:
        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM my_table")
            result = cursor.fetchone()
    """
    connection = None
    try:
        db_url = os.getenv('GEODATA_DATABASE_URL')
        if db_url and db_url.startswith('postgis://'):
            db_url = 'postgresql://' + db_url[len('postgis://'):]
        
        connection = psycopg.connect(db_url)
        cursor = connection.cursor()
        yield cursor
        connection.commit()
        
    except Exception as e:
        if connection:
            connection.rollback()
        print(f"Database error: {e}")
        raise
    
    finally:
        if connection:
            if cursor:
                cursor.close()
            connection.close()


def get_pixel_value_5070(x, y):
    """
    Get the pixel value at a point in EPSG:5070 coordinates.
    
    Args:
        x (float): X coordinate in EPSG:5070
        y (float): Y coordinate in EPSG:5070
    
    Returns:
        int: Pixel value at the specified point
    """
    try:
        with db_cursor() as cursor:
            # Even though we're using EPSG:5070, we need to explicitly set the SRID in the query
            point = f"ST_SetSRID(ST_MakePoint({x}, {y}), 5070)"
            query = f"""
            SELECT ST_Value(rast, {point})
            FROM nlcd_landcover
            WHERE ST_Intersects(rast, {point})
            LIMIT 1;
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else None
            
    except Exception as e:
        print(f"Error getting pixel value: {e}")
        return None

def get_pixel_counts_in_buffer_5070(x, y, buffer_meters=90):
    """
    Get counts of pixel values within a buffer distance of a point in EPSG:5070 coordinates.
    
    Args:
        x (float): X coordinate in EPSG:5070
        y (float): Y coordinate in EPSG:5070
        buffer_meters (float): Buffer distance in meters
    
    Returns:
        dict: Dictionary of {pixel_value: count}
    """
    try:
        with db_cursor() as cursor:
            query = f"""
            WITH 
            -- Create buffer around point (Need to set SRID to 5070 explicitly)
            buffer AS (
                SELECT ST_Buffer(
                    ST_SetSRID(ST_MakePoint({x}, {y}), 5070), 
                    {buffer_meters}
                ) AS geom
            ),
            -- Clip raster with buffer
            clipped AS (
                SELECT ST_Clip(rast, buffer.geom) AS clipped_rast
                FROM nlcd_landcover, buffer
                WHERE ST_Intersects(rast, buffer.geom)
            )
            -- Count pixel values in clipped raster
            SELECT (pvc).*
            FROM (
                SELECT ST_ValueCount(clipped_rast) AS pvc
                FROM clipped
            ) AS pixelcount;
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            return {value: count for value, count in results}
            
    except Exception as e:
        print(f"Error getting pixel counts: {e}")
        return None

def get_pixel_counts_in_neighborhood_5070(x, y, window_size=3):
    """
    Get counts of pixel values within a square neighborhood window of a point in EPSG:5070 coordinates.
    Uses ST_Neighborhood to efficiently get a window of pixels around the target cell.
    
    Args:
        x (float): X coordinate in EPSG:5070
        y (float): Y coordinate in EPSG:5070
        window_size (int): Size of the window in cells (default 3 for 3x3 window which is 90m x 90m assuming 30m cells)
    
    Returns:
        dict: Dictionary of {pixel_value: count}
    """
    try:
        with db_cursor() as cursor:
            # Calculate distance in cells for the neighborhood
            distance = window_size // 2
            
            query = f"""
            WITH 
            -- Get the raster coordinates for our point (returned as columnx, rowy)
            raster_coords AS (
                SELECT 
                    rast,
                    (ST_WorldToRasterCoord(rast, ST_SetSRID(ST_MakePoint({x}, {y}), 5070))).*
                FROM nlcd_landcover
                WHERE ST_Intersects(rast, ST_SetSRID(ST_MakePoint({x}, {y}), 5070))
                LIMIT 1
            )
            -- Get neighborhood and count values directly
            SELECT (pvc).*
            FROM (
                SELECT ST_ValueCount(
                    ST_Neighborhood(rast, 1, columnx, rowy, {distance}, {distance})
                ) AS pvc
                FROM raster_coords
            ) AS pixelcount;
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            return {value: count for value, count in results}
            
    except Exception as e:
        print(f"Error getting pixel counts: {e}")
        return None

def main():
    # San Francisco coordinates in EPSG:5070 (Albers Equal Area)
    x = -2275431.914745045  # Easting in EPSG:5070
    y = 1955935.417137774   # Northing in EPSG:5070
    
    # Get the pixel value counts using both methods
    buffer_counts = get_pixel_counts_in_buffer_5070(x, y)
    neighborhood_counts = get_pixel_counts_in_neighborhood_5070(x, y)
    
    print("Buffer counts:", buffer_counts)
    print("Neighborhood counts:", neighborhood_counts)

if __name__ == "__main__":
    main()
