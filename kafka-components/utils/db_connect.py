from sqlalchemy import create_engine, text
import time
# # Connection settings
# db_user = 'postgres'
# db_pass = 'example'
# db_host = 'db'
# db_port = '5432'
# db_name = 'raw_data'

# SQLAlchemy connection string

def create_db_connection(user: str='postgres', password: str='example', host: str='db', port: str='5432', name: str='raw_data'):
    """
    Establishes and returns a persistent connection to a PostgreSQL database using SQLAlchemy.

    This function attempts to connect to a PostgreSQL database using the provided credentials 
    and connection parameters. If the initial connection attempt fails (e.g., the database 
    is not ready), it will continuously retry every 3 seconds until a connection is established.

    Parameters
    ----------
    user : str, optional
        The username used to authenticate with the database. Default is 'postgres'.
    password : str, optional
        The password associated with the database user. Default is 'example'.
    host : str, optional
        The hostname or IP address of the database server. Default is 'db'.
    port : str, optional
        The port on which the database server is listening. Default is '5432'.
    name : str, optional
        The name of the database to connect to. Default is 'raw_data'.

    Returns
    -------
    sqlalchemy.engine.base.Connection
        A SQLAlchemy Connection object representing an active database connection.

    Raises
    ------
    This function does not raise exceptions during connection failures; instead,
    it retries indefinitely with a 3-second interval between attempts.

    Notes
    -----
    This function is designed to be resilient in environments where the database may not 
    be immediately available (e.g., during container startup). The function blocks 
    execution until a connection is successfully established.
    """

    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}')
    
    connection = None
    while connection is None:
        try:
             connection = engine.connect()
        except Exception as e:
            print(f"DB not ready, retrying in 3 seconds... ({e})")
            time.sleep(3)
    return connection