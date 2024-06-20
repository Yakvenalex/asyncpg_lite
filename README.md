# asyncpg_lite

```markdown
`asyncpg_lite` is a Python library designed to facilitate asynchronous interactions with a PostgreSQL database using
SQLAlchemy and asyncpg. This library provides methods for connecting to the database, creating tables, inserting,
updating, selecting, and deleting data. It also includes functionality for managing table schemas and handling conflict
resolutions during data insertion.

```

## Features

- **Asynchronous Database Operations**: Utilizes asyncio and SQLAlchemy for non-blocking database interactions.
- **Dynamic Table Management**: Create, update, delete, and drop tables dynamically.
- **Conflict Resolution**: Handle unique key conflicts during data insertion with update or ignore strategies.
- **Logging**: Integrated logging to monitor database operations and issues.

## Installation

Install the library using pip:

```bash
pip install --upgrade asyncpg_lite
```

## Usage example

```python
import asyncio
from asyncpg_lite import DatabaseManager
import logging

async def main_work():
    from sqlalchemy import Integer, String
    async with DatabaseManager(db_url= "postgresql://admin:sdaDSfa231@194.2.170.207:5432/my_db", 
                               log_level=logging.DEBUG, 
                               deletion_password="djdahEWE33a@@das") as db:
        await db.create_table(table_name='my_table_name', columns=[
            {"name": "user_id", "type": Integer, "options": {"primary_key": True, "autoincrement": False}},
            {"name": "first_name", "type": String},
            {"name": "last_name", "type": String},
            {"name": "age", "type": Integer},
        ])
        user_info = {'user_id': 1, 'first_name': 'Alexey', 'last_name': 'Yakovenko', 'age': 31}
        await db.insert_data_with_update(table_name='my_table_name',
                                         records_data=user_info,
                                         conflict_column='user_id',
                                         update_on_conflict=False)

        users_info = [{'user_id': 1, 'first_name': 'Alex', 'last_name': 'Yakovenko', 'age': 31},
                      {'user_id': 2, 'first_name': 'Oleg', 'last_name': 'Antonov', 'age': 20},
                      {'user_id': 3, 'first_name': 'Dmitro', 'last_name': 'Pavlych', 'age': 14},
                      {'user_id': 4, 'first_name': 'Ivan', 'last_name': 'Sidorov', 'age': 66},
                      {'user_id': 5, 'first_name': 'John', 'last_name': 'Doe', 'age': 81}]

        await db.insert_data_with_update(table_name='my_table_name',
                                         records_data=users_info,
                                         conflict_column='user_id',
                                         update_on_conflict=True)

        all_data = await db.select_data('my_table_name')
        for i in all_data:
            print(i)

        one_data = await db.select_data('my_table_name', one_dict=True, where_dict={'user_id': 1})
        print(one_data)

        data_with_filters = await db.select_data('my_table_name',
                                                 where_dict=[{'user_id': 1}, {'user_id': 3}, {'last_name': 'Doe'}])

        for i in data_with_filters:
            print(i)

        # delete example 1
        await db.delete_data(table_name='my_table_name', where_dict={'user_id': 3})

        # delete example 2
        await db.delete_data(table_name='my_table_name', where_dict=[{'user_id': 2}, {'user_id': 5}])

        all_data = await db.select_data('my_table_name')
        for i in all_data:
            print(i)


if __name__ == "__main__":
    asyncio.run(main_work())
```

## License

Этот проект лицензируется по лицензии [MIT](https://choosealicense.com/licenses/mit/).
