# Archiving-Service
This script is a python module for archiving data from a MySQL database. It uses the pandas and sqlalchemy libraries to connect to a MySQL database and fetch data. The module also has methods for uploading data to Azure Blob Storage and for uploading the data to another MySQL database. The `Archiving` class is the main class, and it takes in the database configurations, container name, table names, and backup duration. The class has methods such as `delete`, `to_azure`, and `to_db` that perform the archiving operations. The `delete` method deletes data from the database without backing it up, the `to_azure` method uploads data to Azure Blob Storage, and the `to_db` method uploads data to another MySQL database.

Archiving(`db_configs`, `cont_name`, `table_name`, `backup_duration`)
---------------------------------------------------------------------
--param `db_configs`: the configs of the db where the data is currently present.
--param `cont_name`: container name where the table is present.
--param `table_name`: name of the table from which data has to be backed up.
--param `backup_limit`: the time duration (in days) before which the data has to be removed/backed up.

Methods:
=======
delete():
--------
    delete: deletes data from db, without backuing data up first.

to_azure(`blobName`, `container`, `sas_url`, `sasToken`)
------------------------------------------------
    to_azure: uploads data to azure in the form of blobs.

    -- param `blobNames`: list of names of blob as to be uploaded on azure.
    -- param `container`: name of container.
    -- param `sas_url`: sas url.
    -- param `sasToken`: sas token.

to_db(`new_db_configs`, `backup_table_names`)
-----------------------------------------
    to_db: uploads the backup data to the same or a new database.

    -- param `new_db_configs`: new/old database credentials in the form of a dict.
    -- param `backup_table_names`: list of table names where the backup data has to be appended to.
