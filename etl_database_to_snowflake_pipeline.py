from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from snowflake.connector import connect
from datetime import datetime
import mysql.connector
import pandas as pd


dag_id = 'etl_database_to_snowflake_pipeline'
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



def etl_function():
    print("EXTRACTION STARTED")
    extraction_returned_data = extract_data_from_csv()
    print("TRANSFORMATION STARTED")
    transformation_returned_data = transform_data(extraction_returned_data)
    print("LOADING STARTED")
    load_data_to_snowflake(transformation_returned_data)
    print("ALL DONE")


def extract_data_from_csv():
    mysql_params = {
        'host': 'host ip address',
        'user': 'username',
        'password': 'password',
        'database': 'telanganadb',
    }

    conn = mysql.connector.connect(**mysql_params)
    cursor = conn.cursor()

    query = 'SELECT * FROM tourismandculture'
    
    try:
        cursor.execute(query)

        data = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]
        extracted_data = pd.DataFrame(data, columns=columns)

        return extracted_data

    finally:
        cursor.close()
        conn.close()


def transform_data(extracted):
    data = extracted.copy()
    my_data = data.fillna(0)
    return my_data


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
        transformed_data = transformed.copy()
        

        snowflake_table = 'tourismandculture'
        columns = transformed_data.columns.tolist()

        drop_table_statement = f"DROP TABLE IF EXISTS {snowflake_table};"
        cur.execute(drop_table_statement)

        create_table_statement = f"CREATE TABLE {snowflake_table} ("
        for col in columns:
            if col in ['District']:
                create_table_statement += f"{col} VARCHAR(255),"
            else:
                create_table_statement += f"{col} INT,"

        create_table_statement = create_table_statement.rstrip(',')
        create_table_statement += ");"

        cur.execute(create_table_statement)

        for index, row in transformed_data.iterrows():
            values = []
            for value in row:
                if isinstance(value, str):
                    values.append(f"'{value}'")
                else:
                    values.append(str(value))

            column_names = ', '.join(columns)
            row_values = ', '.join(values)

            sql_statement = f"INSERT INTO {snowflake_table} ({column_names}) VALUES ({row_values})"
            
            cur.execute(sql_statement)

        conn.commit()

        print("DATA LOADING SUCCESSFUL!!")
    except Exception as e:
        print(f"Error: {str(e)}")
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
