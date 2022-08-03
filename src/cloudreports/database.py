"""Define Database."""

from google.cloud import bigquery
from google.oauth2 import service_account
import json


class BigQuery(object):
    """Define Google BigQuery.
    
    See 
    https://cloud.google.com/bigquery
    """

    def __init__(self, project, dataset, credentials_file_path):
        if not isinstance(credentials_file_path, str):
            raise ValueError("Pass a string for credentials")
        if not isinstance(project, str):
            raise ValueError("Pass a string for project")
        if not isinstance(dataset, str):
            raise ValueError("Pass a string for dataset")

        self._project = project
        self._dataset = dataset        

        credentials = service_account.Credentials.from_service_account_file(
            credentials_file_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
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
SELECT\n    entity_id,\n    event_moment,\n"""
  
                result_query_str = json.loads(value)

                for keys, values in result_query_str.items():
            
                    sql_query += '    JSON_EXTRACT_SCALAR(entity_data, "$.{}") as `{}`'.format(keys, keys) + ",\n"
                            
                sql_query += f"""FROM (
        SELECT entity_type, entity_id, entity_data, event_moment FROM
            (SELECT entity_type, entity_id,                   
                FIRST_VALUE(entity_data) OVER(
                    PARTITION BY entity_id
                    ORDER BY event_moment DESC
                ) AS entity_data,
                FIRST_VALUE(event_moment) OVER(
                    PARTITION BY entity_id 
                    ORDER BY event_moment DESC
                ) AS event_moment

            FROM `{self._project}.{self._dataset}.brs_audit_partition` 
            WHERE partition_entity_type = ABS(MOD(FARM_FINGERPRINT('{key}'), 4000))
                AND entity_type = '{key}' AND entity_id is not null
            ORDER BY  entity_id, event_moment DESC)    
        GROUP BY entity_type, entity_id, entity_data, event_moment
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


