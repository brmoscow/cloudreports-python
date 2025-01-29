"""Define Database."""

from google.cloud import bigquery
from google.oauth2 import service_account
import json
import clickhouse_driver
import pandas
import base64

class ClickHouse(object):
    """Define ClickHouse.

    See
    https://clickhouse.com/
    """

    def __init__(self, host, database, user, password, verify=None, port=9440, secure=True):
        if not isinstance(host, str):
            raise ValueError("Pass a string for host")
        if not isinstance(database, str):
            raise ValueError("Pass a string for database")
        if not isinstance(user, str):
            raise ValueError("Pass a string for user")
        if not isinstance(password, str):
            raise ValueError("Pass a string for password")

        self._host = host
        self._database = database
        self._user = user
        self._password = password
        # certificate
        self._verify = verify

        self.client = clickhouse_driver.Client(
            host=self._host, port=port, database=self._database, user=self._user,
            password=self._password,  ca_certs=self._verify,
            secure=secure, settings={'use_numpy': True}) 

        self.table_audit = 'brs_audit'
        self.table_audit_partition = 'brs_audit_partition'
        self.table_temp = 'brs_audit_temp'
        self.tables_created = False


    def load_json_data(self, data):
        if not self.tables_created:
            self.create_tables()
            self.tables_created = True

        df = pandas.DataFrame(data)
        # for FIRST_VALUE(event_moment) OVER(PARTITION BY entity_id ORDER BY event_moment2 DESC)
        df['event_moment2'] = df['event_moment']

        self.client.insert_dataframe(f'INSERT INTO {self._database}.{self.table_audit} VALUES', df)

        # brs_audit_temp
        query = f"""
                    CREATE TABLE IF NOT EXISTS {self._database}.brs_audit_temp (
                            entity_href String,
                            entity_id String,
                            entity_type String,
                            entity_data String,
                            event_type String,
                            event_moment DateTime,	
                            event_moment2 DateTime
                    ) ENGINE = MergeTree()
                    order by event_moment"""
        self.client.execute(query)
        self.client.insert_dataframe(f'INSERT INTO {self._database}.{self.table_temp} VALUES', df)
        self.fill_audit_partition('brs_audit_temp')
        self.client.execute(f"drop table if exists {self.table_temp}")


    def create_tables(self):
	    # brs_audit
        query = f"""
                    CREATE TABLE IF NOT EXISTS {self._database}.brs_audit (
                            entity_href String,
                            entity_id String,
                            entity_type String,
                            entity_data String,
                            event_type String,
                            event_moment DateTime,	
                            event_moment2 DateTime
                    ) ENGINE = MergeTree()
                    order by event_moment"""
        self.client.execute(query)

        # brs_audit_partition
        query = f"""
                CREATE TABLE IF NOT EXISTS {self._database}.brs_audit_partition (
                        entity_href String,
                        entity_id String,
                        entity_type String,
                        entity_data String,	
                        event_type String,
                        event_moment DateTime,	
                        event_moment2 DateTime,
                        partition_entity_type Int64) ENGINE = MergeTree
                PARTITION BY partition_entity_type
                order by partition_entity_type"""
        self.client.execute(query)


    def table_exists(self, table):
        if self.client.execute(
                f"SELECT name FROM system.tables WHERE name = '{table}' and database = '{self._database}'"):
            return True
        else:
            return False


    def fill_audit_partition(self, basic_table):
        query = f"""
            INSERT INTO {self._database}.brs_audit_partition
            SELECT 	
                    entity_href,
                    entity_id,
                    entity_type,
                    entity_data,
                    event_type,
                    event_moment,
                    event_moment2,
                    abs(farmFingerprint64(entity_type) % 4000)        
            FROM {self._database}.{basic_table}
            """
        self.client.execute(query)


    def update_tables(self):
        """Adding views for new entity types"""
        # rebuild brs_audit_partition
        self.client.execute(f"drop table if exists {self.table_audit_partition}")
        self.create_tables()
        self.fill_audit_partition('brs_audit')

        # build views
        query = f"""SELECT distinct(entity_type),
                FIRST_VALUE(entity_data) OVER( PARTITION BY entity_type ORDER BY (length(extractAll(ifNull(entity_data,''), '"([^"]*)":')))  desc ) AS entity_data
                FROM (
                    SELECT * from (
                            SELECT entity_type, entity_data,
                            ROW_NUMBER() OVER (PARTITION BY entity_type ORDER BY event_moment desc) rn
                            FROM {self._database}.brs_audit_partition
                        ) t1
                        WHERE t1.rn < 1000
                ) t2"""

        rows = self.client.execute(query)
        result_query = []
        for row in rows:
            for item in row:
                result_query.append(item)
        result_query = dict(zip(result_query[::2], result_query[1::2]))

        for key, value in result_query.items():
            view_id = f"brv_{key}"
            if not self.table_exists(view_id):
                sql_query = f"""create view {self._database}.brv_{key}\nas\nSELECT\nentity_href,\nentity_id,\nevent_moment\n"""

                result_query_str = json.loads(value)

                for keys, values in result_query_str.items():
                    keys2 = keys
                    if keys == 'entity_id': keys2 = 'entity_entity_id' 
                    sql_query += ",JSON_VALUE(entity_data, '$.{}') as `{}`".format(keys, keys2) + "\n"

                sql_query += f"""FROM ( SELECT entity_type, entity_href, entity_id, entity_data, event_moment FROM 
                            (SELECT entity_type, entity_href, 
                            FIRST_VALUE(entity_id) OVER(PARTITION BY entity_href ORDER BY event_moment2 DESC) AS entity_id,
                            FIRST_VALUE(entity_data) OVER(PARTITION BY entity_href ORDER BY event_moment2 DESC) AS entity_data,
                            FIRST_VALUE(event_moment) OVER(PARTITION BY entity_href ORDER BY event_moment2 DESC) AS event_moment
                            FROM {self._database}.brs_audit_partition
                            WHERE partition_entity_type = abs(farmFingerprint64('{key}') % 4000) 
                            AND entity_type = '{key}' 
                            ORDER BY  entity_href, event_moment DESC)
                            GROUP BY entity_type, entity_href, entity_id, entity_data, event_moment )"""

                self.client.execute(sql_query)


    def delete_tables(self, full_delete = True):
        query = f"""SELECT name FROM system.tables WHERE database = '{self._database}'"""
        tables = self.client.execute(query)
        for table in tables:
            if table[0][:3] == 'brv' or (table[0][:3] == 'brs' and full_delete):
                self.client.execute(f"drop table if exists {self._database}.{table[0]}")


    def run_sql(self, query):
        self.client.execute(query)        




class BigQuery(object):
    """Define Google BigQuery.
    
    See 
    https://cloud.google.com/bigquery
    """

    def __init__(self, project, dataset, credentials_file_path=None, credentials_service_account_info=None):
        if not (isinstance(credentials_file_path, str) or isinstance(credentials_service_account_info, str)):
            raise ValueError("Pass a string for credentials_file_path or credentials_service_account_info")
        if not isinstance(project, str):
            raise ValueError("Pass a string for project")
        if not isinstance(dataset, str):
            raise ValueError("Pass a string for dataset")

        self._project = project
        self._dataset = dataset        

        if isinstance(credentials_file_path, str):
            credentials = service_account.Credentials.from_service_account_file(
                credentials_file_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
        elif isinstance(credentials_service_account_info, str):
            credentials = service_account.Credentials.from_service_account_info(json.loads(base64.b64decode(credentials_service_account_info)))   

        self.client = bigquery.Client(project=self._project, credentials=credentials) 
        self.dataset_ref = self.client.dataset(dataset_id=self._dataset)
        self.table_audit = self.dataset_ref.table('brs_audit')  
        self.table_audit_partition = self.dataset_ref.table('brs_audit_partition') 
        self.table_temp = self.dataset_ref.table('brs_audit_temp')  
        self.sandbox_mode = False
        self.tables_created = False
    

    def load_json_data(self, data):        
        if not self.tables_created:            
            self.client.delete_table(self.table_temp, not_found_ok=True)
            self.create_tables()
            self.tables_created = True            

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("entity_href", "STRING"),
                bigquery.SchemaField("entity_id", "STRING"),
                bigquery.SchemaField("entity_type", "STRING"),
                bigquery.SchemaField("entity_data", "STRING"),
                bigquery.SchemaField("event_type", "STRING"),
                bigquery.SchemaField("event_moment", "TIMESTAMP"),
            ],
        )
        
        self.client.load_table_from_json(data, self.table_audit, job_config = job_config).result()
        
        if not self.sandbox_mode:
            self.client.load_table_from_json(data, self.table_temp, job_config = job_config).result()
            self.fill_audit_partition('brs_audit_temp')
            self.client.delete_table(self.table_temp, not_found_ok=True)
        

    def create_tables(self):
        # brs_audit_partition        
        query_job = self.client.query(
            f"""
            CREATE TABLE IF NOT EXISTS `{self._project}.{self._dataset}.brs_audit_partition` (
                    entity_href STRING,
                    entity_id STRING,
                    entity_type STRING,
                    entity_data STRING,
                    event_type STRING,
                    event_moment TIMESTAMP,	
                    partition_entity_type INT64)
            PARTITION BY
            RANGE_BUCKET(partition_entity_type, GENERATE_ARRAY(0, 4000, 1))"""
        )
        query_job.result()
            
       
    def create_tables_for_sandbox(self):
        # brs_audit_partition
        if self.table_exists(self.table_audit):
            sql_query = f"""
            SELECT 	
                entity_href,
                entity_id,
                entity_type,
                entity_data,
                event_type,
                event_moment,	
                ABS(MOD(FARM_FINGERPRINT(entity_type), 4000)) partition_entity_type            
            FROM `{self._project}.{self._dataset}.brs_audit`"""

            view_id = f"{self._project}.{self._dataset}.brs_audit_partition"
            view = bigquery.Table(view_id)
            view.view_query = sql_query
            view = self.client.create_table(view)

  
    def table_exists(self, table):
        try:
            self.client.get_table(table)
            return True
        except:
            return False


    def fill_audit_partition(self, basic_table):        
            try:
                query_job = self.client.query(
                    f"""
                    INSERT INTO `{self._project}.{self._dataset}.brs_audit_partition` 
                    ( 
                        SELECT
                            entity_href, 	
                            entity_id,
                            entity_type,
                            entity_data,
                            event_type,
                            event_moment,	
                            ABS(MOD(FARM_FINGERPRINT(entity_type), 4000))         
                        FROM `{self._project}.{self._dataset}.{basic_table}`) 
                    """
                )

                query_job.result()

            except Exception:
                self.sandbox_mode = True
                self.client.delete_table(self.table_audit_partition, not_found_ok=True)
                self.client.delete_table(self.table_temp, not_found_ok=True)
                self.create_tables_for_sandbox()
    

    def update_tables(self):        
        """Adding views for new entity types"""
        
        # rebuild brs_audit_partition
        self.client.delete_table(self.table_audit_partition, not_found_ok=True)
        self.create_tables()
        self.fill_audit_partition('brs_audit')

        # build views  
        query_bq = f"""SELECT distinct(entity_type),
                FIRST_VALUE(entity_data) OVER(
                PARTITION BY entity_type
                ORDER BY (ARRAY_LENGTH(REGEXP_EXTRACT_ALL(entity_data, r'["\\']([^"]*)["\\'](?:\:)')))  desc 
                ) AS entity_data
                FROM (
                    SELECT * from (
                            SELECT entity_type, entity_data,
                            ROW_NUMBER() OVER (PARTITION BY entity_type ORDER BY event_moment desc) rn
                            FROM `{self._project}.{self._dataset}.brs_audit_partition`
                        ) t1
                        WHERE t1.rn < 1000
                ) t2"""

        query_job = self.client.query(query_bq)
        rows = query_job.result()

        result_query = []
        for row in rows:
            for key, value in row.items():
                result_query.append(value)
        
        result_query = dict(zip(result_query[::2], result_query[1::2]))
        
        for key, value in result_query.items():
            view_id = f"{self._project}.{self._dataset}.brv_{key}"

            if not self.table_exists(view_id):
                sql_query = """-- Use JSON functions to extract values.
-- See https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
SELECT\n    entity_href,\n    entity_id,\n    event_moment,\n"""
  
                result_query_str = json.loads(value)

                for keys, values in result_query_str.items():
            
                    sql_query += '    JSON_EXTRACT_SCALAR(entity_data, "$.{}") as `{}`'.format(keys, keys) + ",\n"
                            
                sql_query += f"""FROM (
        SELECT entity_href, entity_type, entity_id, entity_data, event_moment FROM
            (SELECT entity_href, entity_type,  
                FIRST_VALUE(entity_id) OVER(
                    PARTITION BY entity_href
                    ORDER BY event_moment DESC
                ) AS entity_id,                   
                FIRST_VALUE(entity_data) OVER(
                    PARTITION BY entity_href
                    ORDER BY event_moment DESC
                ) AS entity_data,
                FIRST_VALUE(event_moment) OVER(
                    PARTITION BY entity_href
                    ORDER BY event_moment DESC
                ) AS event_moment

            FROM `{self._project}.{self._dataset}.brs_audit_partition` 
            WHERE partition_entity_type = ABS(MOD(FARM_FINGERPRINT('{key}'), 4000))
                AND entity_type = '{key}' 
            ORDER BY  entity_href, event_moment DESC)    
        GROUP BY entity_href, entity_type, entity_id, entity_data, event_moment
    )
                """            
            
                view = bigquery.Table(view_id)
                view.view_query = sql_query
                view = self.client.create_table(view)               


    def delete_tables(self, full_delete = True):
        tables = self.client.list_tables(self.dataset_ref)  
        for table in tables:
            if table.table_id[:3] == 'brv' or (table.table_id[:3] == 'brs' and full_delete):
                self.client.delete_table(table, not_found_ok=True)


    def run_sql(self, query):
        query_job = self.client.query(query)
        query_job.result()