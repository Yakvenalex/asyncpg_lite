import logging
import re
from typing import Optional, Union, Dict, List, Any
from sqlalchemy import MetaData, Table, Column, select, update, delete, Index
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import and_, or_, func
from sqlalchemy.dialects.postgresql import insert
import urllib.parse


class DatabaseManager:
    def __init__(self, deletion_password: str, db_url: Optional[str] = None, expire_on_commit: bool = True,
                 echo: bool = False, log_level: int = logging.INFO, auth_params: Optional[Dict[str, str]] = None):
        """
        Initializes a database object.

        :param deletion_password: A mandatory parameter used for verification before performing critical operations.
        :param db_url: URL of the database.
        :param expire_on_commit: Flag indicating whether session objects should expire after a commit (default is True).
        :param echo: Flag enabling SQL query output to stdout (default is False).
        :param log_level: Logging level for the database (default is logging.INFO).
        :param auth_params: Dictionary containing authentication parameters.

        Example auth_params:
        {'user': 'user', 'password': 'password', 'host': 'host', 'port': port, 'database': 'database_name'}.
        """
        self.deletion_password = deletion_password
        self.db_url = db_url
        self.engine = None
        self.metadata = MetaData()
        self.Base = declarative_base(metadata=self.metadata)
        self.session = None
        self._session_instance = None
        self.expire_on_commit = expire_on_commit
        self.echo = echo
        self.auth_params = auth_params or {}

        # Configure logging
        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(log_level)
        self.logger.info("Database instance created with log level: %s", logging.getLevelName(log_level))

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.disconnect()

    async def connect(self):
        # Establishing a connection to the database
        if self.db_url:
            # Parse the existing db_url to extract username, password, host, port, and database
            parsed_url = urllib.parse.urlparse(self.db_url)
            password = parsed_url.password
            if password:
                password = urllib.parse.quote_plus(password)

            # Construct the new db_url with encoded components
            db_url_correct = (
                f"postgresql+asyncpg://{parsed_url.username}:{password}"
                f"@{parsed_url.hostname}:{parsed_url.port}"
                f"/{parsed_url.path.lstrip('/')}"
            )
        else:
            # Encode the username and password for safe inclusion in the connection URL
            encoded_user = urllib.parse.quote_plus(self.auth_params['user'])
            encoded_password = urllib.parse.quote_plus(self.auth_params['password'])

            db_url_correct = (
                f"postgresql+asyncpg://{encoded_user}:{encoded_password}"
                f"@{self.auth_params['host']}:{self.auth_params['port']}"
                f"/{self.auth_params['database']}"
            )
        self.engine = create_async_engine(db_url_correct, echo=self.echo)
        self.session = async_sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=self.expire_on_commit)
        self._session_instance = self.session()
        self.logger.info(f"Connect with PostgreSQL success!")

    async def disconnect(self):
        # Closing the connection to the database
        if self._session_instance:
            await self._session_instance.close()
        if self.engine:
            await self.engine.dispose()
        self.logger.info("Disconnected from the database.")

    async def create_table(self, table_name: str, columns: List[Dict[str, Any]]) -> None:
        """Creates a table in the database.

        :param table_name: Name of the table.
        :param columns: List of dictionaries defining the columns.

        Example columns:
        [
            {"name": "user_id", "type": Integer, "options": {"primary_key": True, "autoincrement": False}},
            {"name": "name", "type": String},
            {"name": "age", "type": Integer},
        ]

        Each dictionary in the columns list should have:
        - `name`: Name of the column.
        - `type`: Data type of the column (e.g., Integer, String).
        - `options`: Additional options for the column (e.g., primary_key, autoincrement).

        This method validates the table and column names, creates the table schema,
        and logs the creation process.

        Example usage:
        await db.create_table("users", [
            {"name": "user_id", "type": Integer, "options": {"primary_key": True, "autoincrement": True}},
            {"name": "username", "type": String, "options": {"nullable": False}},
            {"name": "email", "type": String, "options": {"unique": True}},
        ])
        """

        column_objects = []
        indexes = []

        for col in columns:
            col_name = col['name']
            col_type = col['type']
            options = col.get('options', {})

            primary_key = options.get('primary_key', False)
            nullable = options.get('nullable', True)
            default = options.get('default', None)
            onupdate = options.get('onupdate', None)
            unique = options.get('unique', False)
            autoincrement = options.get('autoincrement', None)
            index = options.get('index', False)

            # Using server_default for default values
            server_default = None
            if default is not None:
                if isinstance(default, str):
                    server_default = func.text(default)
                else:
                    server_default = func.text(str(default))

            # Explicitly set column options
            column = Column(
                col_name,
                col_type,
                primary_key=primary_key,
                nullable=nullable,
                server_default=server_default,
                onupdate=onupdate,
                unique=unique,
                autoincrement=autoincrement
            )
            column_objects.append(column)

            # Add indexes
            if index:
                indexes.append(Index(f'ix_{table_name}_{col_name}', column))

        # Creating a table in the database
        table = Table(table_name, self.metadata, *column_objects, *indexes)

        async with self.engine.begin() as conn:
            await conn.run_sync(self.metadata.create_all)

        self.logger.info("Table '%s' created with columns: %s", table_name, [col['name'] for col in columns])

    async def get_table(self, table_name: str) -> Table:
        """
        Retrieves a table object from metadata by table name.

        :param table_name: Name of the table.
        :return: Table object.
        """
        # Getting a table from metadata
        async with self.engine.connect() as conn:
            await conn.run_sync(self.metadata.reflect)
            table = Table(table_name, self.metadata, autoload_with=conn)
        self.logger.debug("Table '%s' fetched from metadata.", table_name)
        return table

    async def select_data(self, table_name: str, where_dict: Optional[Union[Dict, List[Dict]]] = None,
                          columns: Optional[List[str]] = None, one_dict: bool = False) -> Union[List[Dict], Dict]:
        """
        Retrieves data from a database table.

        :param table_name: Name of the table.
        :param where_dict: Conditions to filter the data.
        :param columns: List of columns to retrieve.
        :param one_dict: Whether to return only one record as a dictionary.
        :return: List of dictionaries with data or a single dictionary if one_dict=True.
        """
        # Getting a table from metadata
        table = await self.get_table(table_name)

        # Building a query
        if columns:
            stmt = select(*[table.c[col] for col in columns])
        else:
            stmt = select(table)

        # Adding where conditions
        if where_dict:
            if isinstance(where_dict, Dict):
                conditions = [table.c[key] == value for key, value in where_dict.items()]
                stmt = stmt.where(and_(*conditions))
            elif isinstance(where_dict, List):
                conditions = []
                for condition in where_dict:
                    conditions.append(and_(*[table.c[key] == value for key, value in condition.items()]))
                stmt = stmt.where(or_(*conditions))

        async with self.session() as session:
            async with session.begin():
                result = await session.execute(stmt)
                rows = result.all()

        # Converting results to a dictionary list
        list_of_dicts = [row._asdict() for row in rows]
        if one_dict and list_of_dicts:
            return list_of_dicts[0]
        self.logger.info("Selected %d rows from table '%s'.", len(list_of_dicts), table_name)
        return list_of_dicts

    async def insert_data_with_update(self, table_name: str, records_data: Union[dict, List[dict]],
                                      conflict_column: str, update_on_conflict: bool = True):
        """
        Inserts data into a database table. Handles conflict on unique key by updating or ignoring the record.

        :param table_name: Name of the table.
        :param records_data: Dictionary or list of dictionaries containing data to insert.
        :param conflict_column: Name of the column to check for uniqueness conflict.
        :param update_on_conflict: Flag indicating whether to update the record on conflict (True) or ignore it (False).
        """
        # Получение таблицы из метаданных
        table = await self.get_table(table_name)

        # Преобразуем записи в список, если передан один словарь
        if isinstance(records_data, dict):
            records_data = [records_data]

        if not records_data:
            self.logger.warning("No records to insert.")
            return

        first_record = records_data[0]

        # Построение запроса на вставку с обновлением или игнорированием при конфликте
        stmt = insert(table).values(records_data)
        if update_on_conflict:
            update_dict = {col: getattr(stmt.excluded, col) for col in first_record.keys() if col != conflict_column}
            stmt = stmt.on_conflict_do_update(
                index_elements=[conflict_column],
                set_=update_dict
            )
        else:
            stmt = stmt.on_conflict_do_nothing()

        async with self.session() as session:
            async with session.begin():
                result = await session.execute(stmt)
                await session.commit()

        self.logger.info("Inserted/Updated %d records into table '%s'.", len(records_data), table_name)

    async def update_data(self, table_name: str,
                          where_dict: Union[Dict[str, Union[str, int]], List[Dict[str, Union[str, int]]]],
                          update_dict: Dict[str, Union[str, int]]):
        """
        Updates data in a database table.

        :param table_name: Name of the table.
        :param where_dict: Conditions to select records for update.
        :param update_dict: Dictionary with data to update.
        """
        # Получение таблицы из метаданных
        table = await self.get_table(table_name)

        # Построение запроса на обновление
        stmt = update(table).values(**update_dict)

        # Добавление условий where
        if isinstance(where_dict, dict):
            conditions = [table.c[key] == value for key, value in where_dict.items()]
            stmt = stmt.where(and_(*conditions))
        elif isinstance(where_dict, list):
            conditions = []
            for condition in where_dict:
                conditions.append(and_(*[table.c[key] == value for key, value in condition.items()]))
            stmt = stmt.where(or_(*conditions))

        async with self.session() as session:
            async with session.begin():
                self.logger.debug("Table '%s' fetched from metadata.", table_name)
                await session.execute(stmt)
                await session.commit()

        self.logger.info("Updated records in table '%s' with conditions %s.", table_name, where_dict)

    async def delete_data(self, table_name: str,
                          where_dict: Union[Dict[str, Union[str, int]], List[Dict[str, Union[str, int]]]],
                          ):
        """
        Deletes data from a database table.

        :param table_name: The name of the table.
        :param where_dict: Conditions for selecting records to delete.
        """
        # Получение таблицы из метаданных
        table = await self.get_table(table_name)

        # Построение запроса на удаление
        stmt = delete(table)

        # Добавление условий where
        if isinstance(where_dict, dict):
            conditions = [table.c[key] == value for key, value in where_dict.items()]
            stmt = stmt.where(and_(*conditions))
        elif isinstance(where_dict, list):
            conditions = []
            for condition in where_dict:
                conditions.append(and_(*[table.c[key] == value for key, value in condition.items()]))
            stmt = stmt.where(or_(*conditions))

        async with self.session() as session:
            async with session.begin():
                self.logger.debug(f'SQL: {str(stmt.compile(compile_kwargs={"literal_binds": True}))}')
                await session.execute(stmt)
                await session.commit()

        self.logger.info("Deleted records from table '%s' with conditions %s.", table_name, where_dict)

    async def drop_table(self, table_name: str, password: str) -> None:
        """
        Drops a table from the database.

        :param table_name: Name of the table to drop.
        :param password: A mandatory parameter used for verification before performing critical operations.
        """

        if password != self.deletion_password:
            self.logger.warning(f"Wrong password! Drop table {table_name} cancel.")
            return

        # Retrieve the table from metadata
        table = await self.get_table(table_name)

        # Drop the table
        async with self.engine.begin() as conn:
            await conn.run_sync(self.metadata.drop_all, tables=[table])
        self.logger.info("Table '%s' dropped.", table_name)

    async def delete_all_data(self, table_name: str, password: str) -> None:
        """
        Deletes all data from a table in the database.

        :param table_name: Name of the table.
        :param password: A mandatory parameter used for verification before performing critical operations.
        """
        if password != self.deletion_password:
            self.logger.warning("Wrong password! Delete data cancel.")
            return

        # Retrieve the table from metadata
        table = await self.get_table(table_name)

        # Build delete statement
        stmt = delete(table)

        # Execute delete statement
        async with self.session() as session:
            async with session.begin():
                await session.execute(stmt)
                await session.commit()

        self.logger.info("Deleted all data from table '%s'.", table_name)
