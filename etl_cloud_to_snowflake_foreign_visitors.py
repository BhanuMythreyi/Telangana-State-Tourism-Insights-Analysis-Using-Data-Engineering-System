from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from snowflake.connector import connect
from datetime import datetime
import pandas as pd
import io

dag_id = 'etl_cloud_to_snowflake_foreign_visitors'
schedule_interval = None
default_args = {
    'owner': 'Admin',
    'start_date': datetime(2024, 1, 29),
    'retries': 1,
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,
)

s3_bucket_name = 'foreignvisitorsbucket'
aws_access_key = 'Access Key'  
aws_secret_key = 'Secret Key'
aws_conn_id='amazon_conn'  


def etl_function():
    print("EXTRACTION STARTED")
    extraction_returned_data = extract_data_from_s3(s3_bucket_name, aws_conn_id)
    print("TRANSFORMATION STARTED")
    transformation_returned_data = transform_data(extraction_returned_data)
    print("LOADING STARTED")
    load_data_to_snowflake(transformation_returned_data)
    print("ALL DONE")


def extract_data_from_s3(bucket_name, aws_conn_id):
    try:
        s3_hook = S3Hook(aws_conn_id)
        s3_objects = s3_hook.list_keys(bucket_name=bucket_name)

        if not s3_objects:
            raise Exception(f"No objects found in the S3 bucket '{bucket_name}'.")

        latest_object = max(s3_objects)
        file_content = s3_hook.read_key(latest_object, bucket_name)

        data_frame = pd.read_csv(io.StringIO(file_content))
        print("EXTRACTION DONE")
        return data_frame

    except Exception as e:
        print(f"Error in extract_data_from_s3: {str(e)}")
        raise


def transform_data(extracted):
    try:
        data_frame = extracted.copy()
        all_columns = data_frame.columns.tolist()
        non_visitor_columns = ['District','Month']
        visitor_columns=[]
        for i in all_columns:
            if i not in non_visitor_columns:
                visitor_columns.append(i)

        for col in visitor_columns:
            new_col_values = []
            for value in data_frame[col]:
                if pd.isna(value) or not value.strip().isdigit():
                    new_col_values.append(0)
                else:
                    new_col_values.append(int(value))

            data_frame[col] = new_col_values

        district_sums = {}
        district_counts = {}

        for index, row in data_frame.iterrows():
            district = row['District']
            for col in visitor_columns:
                if row[col] != 0:
                    key = (district, col)

                    if key not in district_sums:
                        district_sums[key] = 0
                        district_counts[key] = 0

                    district_sums[key] += row[col]
                    district_counts[key] += 1

        district_means = {}
        for (district, col), value_sum in district_sums.items():
            count = district_counts[(district, col)]
            if district not in district_means:
                district_means[district] = {}

            if count > 0:
                district_means[district][col] = value_sum // count
            else:
                district_means[district][col] = 0

        modified_data_frame = data_frame.copy()
        for index, row in data_frame.iterrows():
            district = row['District']
            for col in visitor_columns:
                if row[col] == 0:
                    if district in district_means and col in district_means[district]:
                        modified_data_frame.at[index, col] = district_means[district][col]
                    else:
                        modified_data_frame.at[index, col] = 0

        print("TRANSFORMATION DONE")
        return modified_data_frame

    except Exception as e:
        print(f"Error in transform_data: {str(e)}")
        raise



def load_data_to_snowflake(transformed):
    snowflake_params = {
        "account": "abcd.ap-southeast-1",
        "user": "username",
        "password": "Password",
        "warehouse": "dataengineering",
        "database": "telangana",
        "schema": "tourism",
    }

    conn = connect(**snowflake_params)
    cur = conn.cursor()

    try:
        snowflake_table = 'foreignvisitorstable'
        columns = transformed.columns.tolist()

        drop_table_statement = f"DROP TABLE IF EXISTS {snowflake_table};"
        cur.execute(drop_table_statement)

        create_table_statement = f"CREATE TABLE {snowflake_table} ("
        for col in columns:
            if col in ['District', 'Month']:
                create_table_statement += f"{col} VARCHAR(255),"
            else:
                create_table_statement += f"{col} INT,"

        create_table_statement = create_table_statement.rstrip(',')
        create_table_statement += ");"


        cur.execute(create_table_statement)

        for index, row in transformed.iterrows():
            values = []
            for value in row:
                if isinstance(value, str):
                    values.append(f"'{value}'")
                else:
                    values.append(str(value))

            column_names = ', '.join(columns)
            row_values = ', '.join(values)
            
            sql_statement = f"INSERT INTO {snowflake_table} ({column_names}) VALUES ({row_values})"
            
            print(f"SQL statement: {sql_statement}")
            cur.execute(sql_statement)
        
        conn.commit()
        print("LOADING DONE")
    except Exception as e:
        print(f"Error in load_data_to_snowflake: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

extract_transform_load = PythonOperator(
    task_id='extract_transform_load',
    python_callable=etl_function,
    dag=dag,
)

start = DummyOperator(task_id='start')
end = DummyOperator(task_id='end')

start >> extract_transform_load >> end 
