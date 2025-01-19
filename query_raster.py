import psycopg
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def get_database_connection():
    """Create and return a database connection."""
    try:
        connection = psycopg.connect(
            os.getenv('GEODATA_DATABASE_URL')
        )
        return connection
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def get_raster_srid():
    """Query and return the SRID (CRS) of the NLCD landcover raster."""
    connection = get_database_connection()
    if not connection:
        return None
    
    try:
        cursor = connection.cursor()
        
        # Query to get the SRID of the raster
        query = """
        SELECT ST_SRID(rast) 
        FROM nlcd_landcover 
        LIMIT 1;
        """
        
        cursor.execute(query)
        srid = cursor.fetchone()[0]
        
        return srid
        
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    
    finally:
        if connection:
            cursor.close()
            connection.close()

def main():
    srid = get_raster_srid()
    if srid:
        print(f"The SRID (Coordinate Reference System) of the raster is: {srid}")
    else:
        print("Failed to retrieve the SRID.")

if __name__ == "__main__":
    main()
