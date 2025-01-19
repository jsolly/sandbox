import psycopg
from dotenv import load_dotenv
import os
from contextlib import contextmanager
import timeit
import matplotlib.pyplot as plt
import numpy as np

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
            WITH raster_coords AS (
                SELECT 
                    rast,
                    (ST_WorldToRasterCoord(rast, ST_SetSRID(ST_MakePoint({x}, {y}), 5070))).*
                FROM nlcd_landcover
                WHERE ST_Intersects(rast, ST_SetSRID(ST_MakePoint({x}, {y}), 5070))
                LIMIT 1
            ),
            neighborhood AS (
                SELECT unnest(ST_Neighborhood(rast, 1, columnx, rowy, {distance}, {distance})) as pixel_value
                FROM raster_coords
                WHERE ST_Neighborhood(rast, 1, columnx, rowy, {distance}, {distance}) IS NOT NULL
            )
            SELECT pixel_value, count(*) 
            FROM neighborhood 
            WHERE pixel_value IS NOT NULL
            GROUP BY pixel_value;
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            return {value: count for value, count in results}
            
    except Exception as e:
        print(f"Error getting neighborhood values: {e}")
        return None

def benchmark_methods_by_size(x, y, sizes, n_iterations=5):
    """
    Benchmark both methods across different sizes.
    
    Args:
        x (float): X coordinate in EPSG:5070
        y (float): Y coordinate in EPSG:5070
        sizes (list): List of sizes to test (in meters for buffer, cells for neighborhood)
        n_iterations (int): Number of iterations for each size
    
    Returns:
        dict: Dictionary containing timing results for each size
    """
    results = {
        'buffer': {size: [] for size in sizes},
        'neighborhood': {size: [] for size in sizes}
    }
    
    for size in sizes:
        print(f"\nTesting size: {size}")
        for _ in range(n_iterations):
            # Time buffer method (size in meters)
            buffer_start = timeit.default_timer()
            get_pixel_counts_in_buffer_5070(x, y, buffer_meters=size)
            results['buffer'][size].append(timeit.default_timer() - buffer_start)
            
            # Time neighborhood method (size in cells)
            # Convert meters to cells (NLCD is 30m resolution)
            cells = max(3, round(size / 30))  # minimum 3x3 window
            if cells % 2 == 0:  # ensure odd number for centered window
                cells += 1
            neighborhood_start = timeit.default_timer()
            get_pixel_counts_in_neighborhood_5070(x, y, window_size=cells)
            results['neighborhood'][size].append(timeit.default_timer() - neighborhood_start)
    
    return results

def plot_scaling_comparison(results, sizes):
    """
    Create and save a bar chart comparing performance of both methods.
    Shows median execution time for each method side by side.
    """
    plt.figure(figsize=(12, 6))
    
    # Calculate median times for each size
    buffer_medians = [np.median(results['buffer'][size]) for size in sizes]
    neighborhood_medians = [np.median(results['neighborhood'][size]) for size in sizes]
    
    # Set up bar positions
    x = np.arange(len(sizes))
    width = 0.35
    
    # Create bars - neighborhood first (orange), then buffer (blue)
    plt.bar(x - width/2, neighborhood_medians, width, label='Neighborhood Method',
            color='#F39C12', alpha=0.8)
    plt.bar(x + width/2, buffer_medians, width, label='Buffer Method', 
            color='#2E86C1', alpha=0.8)
    
    # Customize plot
    plt.title('Performance Comparison: Buffer vs Neighborhood Methods', 
              fontsize=14, pad=20)
    plt.xlabel('Window Size (meters)', fontsize=12, labelpad=10)
    plt.ylabel('Median Execution Time (seconds)', fontsize=12, labelpad=10)
    
    # Set x-axis ticks and labels
    plt.xticks(x, [str(s) for s in sizes], rotation=45)
    
    # Format y-axis
    min_time = min(min(buffer_medians), min(neighborhood_medians))
    max_time = max(max(buffer_medians), max(neighborhood_medians))
    margin = (max_time - min_time) * 0.1
    plt.ylim(min_time - margin, max_time + margin)
    plt.gca().yaxis.set_major_formatter(plt.FormatStrFormatter('%.3f'))
    
    # Add legend and grid
    plt.legend(loc='upper left', fontsize=10)
    plt.grid(True, axis='y', linestyle='--', alpha=0.3)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save plot with high quality settings
    plt.savefig('performance_comparison.png', dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()

def main():
    # San Francisco coordinates in EPSG:5070 (Albers Equal Area)
    x = -2275431.914745045  # Easting in EPSG:5070
    y = 1955935.417137774   # Northing in EPSG:5070
    
    # Define sizes to test (in meters)
    # Testing from 100m to 200km
    sizes = [100, 1_000, 5_000, 20_000, 50_000, 100_000, 200_000]  # 100m to 200km
    
    # Run benchmarks (5 iterations each)
    print("\nRunning scaling benchmarks...")
    results = benchmark_methods_by_size(x, y, sizes, n_iterations=5)
    
    # Print summary statistics
    print("\nPerformance Summary:")
    for size in sizes:
        buffer_median = np.median(results['buffer'][size])
        neigh_median = np.median(results['neighborhood'][size])
        
        # Convert to km for display if size >= 1000
        display_size = f"{size/1000:.1f}km" if size >= 1000 else f"{size}m"
        print(f"\nSize: {display_size}")
        print(f"Buffer Method: {buffer_median:.3f}s")
        print(f"Neighborhood Method: {neigh_median:.3f}s")
    
    # Create visualization
    plot_scaling_comparison(results, sizes)
    print("\nScaling comparison plot saved as 'performance_comparison.png'")

if __name__ == "__main__":
    main()
