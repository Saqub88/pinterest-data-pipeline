'''
sql_engine_connector.py

This file contains the class which is used to initialise an sqlalchemy engine
which connects to an RDS instance. The engine object can then be utilised by
other classes or methods in order to interact with the database.
'''

import yaml
import sqlalchemy

class AWSDBConnector:
    '''
    This class to be used for connecting and interacting with an Amazon RDS 
    instance. In order to be used, the class must be initialised with a valid
    configuration file. The configuration file must contain all the required 
    credentials to connect to databse required for this project.

    Attributes:
        credentials_file_path (str): Path to YAML file of database credentials.
        database_credentials (dict): Dictionary of credentials for database access.
        engine (sqlalchemy.engine): Engine used to connect to Amazon RDS instance.
    '''

    def __init__(self, credentials_path):
        '''
        Initialises the instance based on the path provided to a YAML file
        containing database connection details.

        Args:
            credentials_path (str): The path to  the YAML file holding the  
            database connection information.
        '''
        self.credentials_file_path = credentials_path
        self.database_credentials = None
        self.engine = None
    
    
    def read_db_creds(self):
        '''Reads in the database connection details from the YAML file.'''
        with open(self.credentials_file_path, 'r') as file:
            self.database_credentials = yaml.safe_load(file)

    
    def init_db_engine(self):
        '''Initialises SQLAlchemy engine using the database credentials.'''
        self.engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.database_credentials['USER']}"
            f":{self.database_credentials['PASSWORD']}"
            f"@{self.database_credentials['HOST']}"
            f":{self.database_credentials['PORT']}"
            f"/{self.database_credentials['DATABASE']}?charset=utf8mb4"
        )