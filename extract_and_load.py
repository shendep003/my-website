import logging
import pandas as pd
import os
import sys
import time
from pis.utils.utils import Config
from pis.utils.database import ArangoConnect, SQLServerConnect
from pis.utils import utils
import shutil
from pis.utils import storage

config_obj = Config()

class ExtractAndLoad:
    """
    Extracts data from ArangoDB using pagination and save to SQL.
    """
    def __init__(self):
        """
        Initializes ArangoDB client and Azure Blob Storage client.
        """
        self.config = config_obj.get_config("bom_analyser")
        sql_obj = SQLServerConnect("Uscldasdwi22")
        self.sql_engine = sql_obj.create_engine()
        self.storage_config = config_obj.get_config("storage")["azure_blob"]

    def run(self, query_name, organization_name):
        """
        Extracts data from ArangoDB in batches, saves to Excel, and uploads to SQL Server.
        
        :param query_name: `str` - Query name from YAML config.
        """
        logging.info(f"Starting paginated data extraction for {query_name}")
        batch_number = 0
        total_records = 0
        start_time = time.time()
        all_dataframes = []
        for df in self.extract_data_paginated(query_name, organization_name):
            if df.empty:
                break
            all_dataframes.append(df)
            total_records += len(df)
            batch_number += 1
        if all_dataframes:
            final_df = pd.concat(all_dataframes, ignore_index=True)
            utils.delete_records_sql(self.sql_engine,
                                 self.config[query_name]['table_name'], f"Organization = '{organization_name}'")
            self.upload_to_sql(final_df, query_name)
            elapsed_time = time.time() - start_time
            logging.info(f"Extracted & uploaded {total_records} records in {elapsed_time:.2f} seconds.")
            return "Uploaded!!"
        else:
            logging.info("No data extracted, skipping upload.")
            return None

    def get_part_ids(self, organization, source_name, file_name):
        """
        method to get part_ids from any given source and file name.
        """
        latest_blob = utils.get_latest_azure_blob(
            self.storage_config["account_name"],
            self.storage_config["account_key"],
            rf"\A{source_name}/\d{{8}}/{file_name}_\d{{6}}.parquet",
            self.storage_config["container_name"],
            source_name
        )
        latest_blob += "/part-"
        logging.info(f"Latest blob for {file_name}: {latest_blob}")
        os.makedirs("/tmp/part_info", exist_ok=True)
        storage.download_multiple_blobs(
            self.storage_config["account_name"],
            self.storage_config["account_key"],
            self.storage_config["container_name"],
            latest_blob,
            "/tmp/part_info"
        )
        df = pd.read_parquet("/tmp/part_info/")
        shutil.rmtree("/tmp/part_info")
        logging.info(f"Loaded rows from {file_name}: {len(df)} {df.columns}")
        if ("item_number" not in df.columns and "parent_item" not in df.columns) or "organization" not in df.columns:
            raise ValueError("Missing 'item_number' or 'organization' columns in parquet file.")
        filtered_df = df[df["organization"] == organization]
        if "item_number" in df.columns:
            part_ids = filtered_df["item_number"].dropna().drop_duplicates().tolist()
        else:
            part_ids = filtered_df["parent_item"].dropna().drop_duplicates().tolist()
        logging.info(f"Filtered {len(part_ids)} part_ids for {organization} from {file_name}")
        return part_ids

    def extract_data_paginated(self, query_name, organization_name):
        """
        Uses ArangoDB AQL pagination to extract data efficiently.
        
        :param query_name: `str` - Query name from YAML config.
        :return: `Generator` yielding DataFrame chunks.
        """
        logging.info(f"Executing paginated query for {query_name}")
        base_query = self.config[query_name]["query"]

        if query_name == "Cat_Master":
            source_name, file_name = "syteline", "part_plant_stage"
        elif query_name == "Product_Structure":
            source_name, file_name = "syteline", "part_bom_stage"
        else:
            raise ValueError(f"Unknown query name: {query_name}")

        part_ids = self.get_part_ids(organization_name, source_name, file_name)

        if not part_ids:
            logging.warning(f"No part IDs found for {organization_name}")
            return

        batch_size = 2000
        arango_conn = ArangoConnect()
        db = arango_conn.get_conn()
        
        for batch_num, start in enumerate(range(0, len(part_ids), batch_size), start=1):
            part_ids_batch = part_ids[start:start + batch_size]
            try:
                logging.info(f"Processing batch {batch_num} with {len(part_ids_batch)} IDs")
                cursor = db.aql.execute(
                    base_query,
                    bind_vars={
                        "part_ids": part_ids_batch,
                        "organization": organization_name
                    },
                    batch_size=batch_size
                )
                results = list(cursor)
                if not results:
                    logging.info(f" Batch {batch_num} returned no results.")
                    continue
                df = pd.DataFrame(results)
                logging.info(f"Fetched {len(df)} records from batch {batch_num}")
                yield df
            except Exception as e:
                logging.error(f"Error in batch {batch_num}: {e}", exc_info=True)
                raise

    def upload_to_sql(self, df, extract_type):
        """
        Uploads the extracted DataFrame to SQL Server.
        
        :param df: `pandas.DataFrame` - DataFrame containing the extracted data.
        :param extract_type: `str` - Type of extraction, used to fetch configuration settings.
        :return: None
        """
        logging.info(f"Uploading to SQL for - {extract_type}")
        utils.write_pandas_df_to_sql(df, self.sql_engine, self.config[extract_type]["table_name"])
        logging.info("Data exported to SQL Server!")