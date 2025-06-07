import dask
from dask.distributed import Client, LocalCluster
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def print_versions():
    """Print version information for debugging"""
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Dask version: {dask.__version__}")
    try:
        import distributed
        logger.info(f"Distributed version: {distributed.__version__}")
    except ImportError:
        logger.error("Could not import distributed package")

def test_dask_connection():
    """Test Dask connection with detailed error reporting"""
    print_versions()
    
    # First try connecting to existing scheduler
    logger.info("\nAttempting to connect to existing Dask scheduler...")
    try:
        client = Client("dask-scheduler:8786")
        logger.info("Successfully connected to Dask scheduler!")
        logger.info(f"Dashboard link: {client.dashboard_link}")
        logger.info(f"\nCluster info:\n{client.cluster}")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to scheduler: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        
        # Try creating local cluster as fallback
        logger.info("\nAttempting to create local cluster...")
        try:
            cluster = LocalCluster(processes=True)
            client = Client(cluster)
            logger.info("Successfully created local cluster!")
            logger.info(f"Dashboard link: {client.dashboard_link}")
            logger.info(f"\nCluster info:\n{client.cluster}")
            return client
        except Exception as e2:
            logger.error(f"Failed to create local cluster: {str(e2)}")
            logger.error(f"Error type: {type(e2)}")
            return None

if __name__ == "__main__":
    client = test_dask_connection()
    if client:
        logger.info("\nTest completed successfully!")
        client.close()
    else:
        logger.error("\nTest failed!") 