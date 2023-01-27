'''
Developed by: Pranjal Ghildiyal and Hardik Seth.
Date created: 12/29/2022

'''

import pandas as pd
import logging as lg
from sqlalchemy import create_engine
from azure.storage.blob import BlobClient
from urllib.parse import urlparse
from .Config.index import read_config_file


# function to connect with sql server and database we have created
def connect_with_sql(sql_username, sql_password, sql_ip, sql_port, sql_database):
    lg.info("Start for making sql_conn with %s, %s, %s, %s, %s" , sql_username, sql_password, sql_ip, sql_port, sql_database)
    try:
        if sql_port == "0" or sql_port == 0:
            sql_port = "3306"
        connect_query = "mysql+pymysql://"+sql_username+":"+sql_password+"@"+sql_ip+":"+sql_port+"/"+sql_database
        engine = create_engine(connect_query)
        lg.info("Execution success with engine: %s", engine)
        return (True, engine)
    except Exception as e:
        lg.error("Execution failure")
        lg.exception("Exception: " + str(e))
        return (False, "Failed to connect")
# function to fetch information from the database
def fetch_details(query,engine):
    lg.info("Start fetch_details with %s, %s" , query,engine)
    try:
        data = pd.read_sql(query,engine)
        lg.info("Execution success: Required data imported from db")
        return (True, data)
    except Exception as e:
        lg.error("Execution failure")
        lg.exception("Exception: " + str(e))
        return (False, "No info fetched")

def df_to_sql(data, name, engine, index = False, how = 'append'):
    try: 
        data.to_sql(name = name, con = engine, if_exists= how, index = index)
        lg.info('{} sent to sql successfully!\n'.format(name))
        return (True, engine)
    except Exception as e:
        lg.warning('{} export unsuccessful! Error message:{}'.format(name, e))

class Archiving():
    '''
    Archiving(`db_configs`, `cont_name`, `table_name`, `backup_duration`)
    =====================================================================
    - db_configs: the configs of the db where the data is currently present.
    - cont_name: container name where the table is present.
    - table_name: name of the table from which data has to be backed up.
    - backup_limit: the time duration (in days) before which the data has to be removed/backed up.
    
    -------------------------------------------------------------------------------

    Methods:
    --------

    - delete():\n
        delete: deletes data from db, without backuing data up first.\n

    - to_azure(`blobName`, `container`, `sas_url`, `sasToken`)\n
        to_azure: uploads data to azure in the form of blobs.\n
        
        -- param `blobNames`: list of names of blob as to be uploaded on azure.
        -- param `container`: name of container.
        -- param `sas_url`: sas url.
        -- param `sasToken`: sas token.
    
    - to_db(`new_db_configs`, `backup_table_names`)\n
        to_db: uploads the backup data to the same or a new database.\n

        -- param `new_db_configs`: new/old database credentials in the form of a dict.
        -- param `backup_table_names`: list of table names where the backup data has to be appended to.
    ----------------------------------------------------------------------------------------------
    '''

    def __init__(self, db_configs, cont_name, table_names, backup_duration):

        self.cont_name = cont_name
        self.table_names = table_names
        self.sql_username = db_configs['sql_username']
        self.sql_password = db_configs['sql_password']
        self.sql_ip = db_configs['sql_ip']
        self.sql_port= db_configs['sql_port']
        self.sql_database = db_configs['sql_database']

        present_timestamp = pd.to_datetime(pd.Timestamp.now(tz='Asia/Kolkata'))
        self.present_datetime = pd.to_datetime('2022-03-30 09:10:00')

        self.backup_limit = self.present_datetime - pd.Timedelta(days = backup_duration)
        self.backup_limit = self.backup_limit.strftime('%Y-%m-%d %H:%M:%S')

    def delete(self):
        '''
        delete(self)
        deletes data from db without creating any kind of backup.
        '''
        #Now deleting the backup file from Original file
        for table_name in self.table_names:
            var = connect_with_sql(self.sql_username, self.sql_password, self.sql_ip, self.sql_port, self.sql_database)
            var[1].execute("DELETE FROM {}.{} WHERE Date_time < '{}';".format(self.sql_database, table_name, self.backup_limit))
            print('backup of {} from {} has been deleted.\n'.format(table_name, self.sql_database))
        var[1].dispose()
    
    def to_azure(self, blobNames, container, sas_url, sasToken):
        '''
        to_azure(self, blobName, container, sas_url, sasToken)
        to_azure: uploads data to azure
        
        param data: pandas dataframe.
        param blobName: list of names of blob as to be uploaded on azure.
        param container: name of container.
        param sas_url: sas url.
        param sasToken: sas token.
        '''

        sasUrlParts = urlparse(sas_url)
        accountEndpoint = sasUrlParts.scheme + '://' + sasUrlParts.netloc

        flag = 0
        if 'backup' not in vars(self):
            flag = 1
            self.backup = {}

        for (table_name, blobName) in zip(self.table_names, blobNames):

            blobSasUrl = accountEndpoint + '/' + container + '/' + blobName + '?' + sasToken

            if flag==1:
                print('Importing Backup Data.')
                var = connect_with_sql(self.sql_username, self.sql_password, self.sql_ip, self.sql_port, self.sql_database)
                query = "select * from {}.{} where Date_time < '{}';".format(self.sql_database, table_name, self.backup_limit)      
                status_n, backup = fetch_details(query, var[1])
                if (not status_n):
                    print('{} not found.\n'.format(table_name))
                    continue
                self.backup[table_name] = backup
            
            blob_client = BlobClient.from_blob_url(blobSasUrl)
            return_statements = blob_client.upload_blob(data=self.backup[table_name].to_csv(index=False))
            print('{} sent to azure as {}'.format(table_name, blobName))
            
        return self

    def to_db(self, new_db_configs, backup_table_names):
        '''
        to_db(new_db_configs, backup_table_name)
        to_db: uploads the backup data to the same/ a new database.

        param new_db_configs: new/old database credentials in the form of a dict.
        param backup_table_name: list of table names where the backup data has to be appended to.
        '''
        sql_username = new_db_configs['sql_username']
        sql_password = new_db_configs['sql_password']
        sql_ip = new_db_configs['sql_ip']
        sql_port= new_db_configs['sql_port']
        sql_database =new_db_configs['sql_database']

        flag = 0
        if 'backup' not in vars(self):
            flag = 1
            self.backup = {}

        for (table_name, backup_table_name) in zip(self.table_names, backup_table_names):

            if flag==1:
                print('Backing up {}'.format(table_name))
                print('Importing Backup Data for {}.'.format(table_name))
                var = connect_with_sql(self.sql_username, self.sql_password, self.sql_ip, self.sql_port, self.sql_database)
                query = "select * from {}.{} where Date_time < '{}';".format(self.sql_database, table_name, self.backup_limit)      
                status_n, backup = fetch_details(query, var[1])
                if (not status_n):
                    print('{} not found.\n'.format(table_name))
                    return None
                    
                self.backup[table_name] = backup
                print('\tBacked up.')
            
            

            var = connect_with_sql(sql_username, sql_password, sql_ip, sql_port, sql_database)
            print('Storing backup to db.')
            df_to_sql(self.backup[table_name], backup_table_name, var[1], index= False)
            print('{} exported to {} successfully.\n'.format(table_name, backup_table_name))
        return self

def archive():
    """
    archive(instruction_list)\n
    instruction_list: contains a list of instructions containing only: 
    - `delete`
    - `to_azure`
    - `to_db`\n
    as strings.\n
    in the order that the user wants it.
    """

    __, db_configs = read_config_file('db')
    ____, scheduler = read_config_file('scheduler')
    instruction_list = scheduler['instructions']
    instruction_list = instruction_list.replace(" ", "")
    instruction_list = instruction_list.split(',')

    if 'to_azure' in instruction_list:
        _, azureTransfer = read_config_file('azure_transfer')
        cont_name = azureTransfer['container_name']
        sas_url = azureTransfer['sas_url']
        sasToken = azureTransfer['sas_token']
        blobNames = azureTransfer['blob_names']
        blobNames = blobNames.replace(" ", "")
        blobNames = blobNames.split(',')
    
    if 'to_db' in instruction_list:
        ___, archiving_configs = read_config_file('archiving')
        table_names = archiving_configs['table_names']
        table_names = table_names.split(',')
        backup_duration = float(archiving_configs['backup_duration'])
        backup_table_names = archiving_configs['backup_table_names']
        backup_table_names = backup_table_names.replace(" ", "")
        backup_table_names = backup_table_names.split(',')

        _____, backup_db_configs = read_config_file('backup_db')

    instance = Archiving(db_configs, cont_name, table_names, backup_duration)
    counter = 1
    
    for instruction in instruction_list:
        print('---------------------{}------------------------'.format(instruction))
        if instruction == 'delete':
            instance.delete()
            counter+=1
            print('--------------------------------------------------------')
            break
        elif instruction == 'to_db':
            instance.to_db(backup_db_configs, backup_table_names)
            counter+=1
            print('--------------------------------------------------------')
            continue
        elif instruction == 'to_azure':
            instance.to_azure(blobNames, cont_name, sas_url, sasToken)
            counter+=1
            print('--------------------------------------------------------')
            continue
        else: 
            print('INVALID INSTRUCTION ON POSITION {}\n'.format(counter))
            break
