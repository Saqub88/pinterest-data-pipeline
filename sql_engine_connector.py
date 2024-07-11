"""
sql_engine_connector.py

This file contains the class which is used to initialise an sqlalchemy engine
which connects to an RDS instance. The engine object can then be utilised by
other classes or methods in order to interact with the database.
"""

import yaml
import sqlalchemy


class AWSDBConnector:
    """
    Class to be used for connecting and interacting with an Amazon RDS 
    instance. In order to be used, the class must be initialised with a valid
    configuration file. The configuration file must contain all the required 
    credentials to connect to databse required for this project.

    Attributes:
        database_credentials (dict): Dictionary of credentials for database
        access.
        engine (sqlalchemy.engine): Engine used to connect to Amazon RDS 
        instance.
    """

    def __init__(self, credentials_path):
        """
        Initialises the instance based on the path provided to a YAML file
        containing database connection details. The method will test the keys 
        contained within yaml file to ensure they meet requirements. Any 
        mismatch of the required keys will cause the program to terminate and
        the class will not be created. The checks do not validate the 
        credentials themselves. With these in place the method will also run 
        the init_db_engine() method.
        
        Args:
            credentials_path (str): The path to  the YAML file holding the  
            database connection information.

        Raises:
            KeyError: If any of the required fields are missing from the YAML
            file.
        """
        self.database_credentials = None
        with open(credentials_path, "r") as file:
            self.database_credentials = yaml.safe_load(file)
        if list(self.database_credentials.keys()) != [
            "HOST",
            "USER",
            "PASSWORD",
            "DATABASE",
            "PORT"
        ]:
            raise KeyError(
                "The credentials file do not contain the following keys"
                "\n 'HOST', 'USER', 'PASSWORD', 'DATABASE', 'PORT'"
            )
        self.engine = None
        self.init_db_engine()

    def init_db_engine(self):
        """Initialises SQLAlchemy engine using the database credentials."""
        self.engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.database_credentials['USER']}"
            f":{self.database_credentials['PASSWORD']}"
            f"@{self.database_credentials['HOST']}"
            f":{self.database_credentials['PORT']}"
            f"/{self.database_credentials['DATABASE']}?charset=utf8mb4"
        )
