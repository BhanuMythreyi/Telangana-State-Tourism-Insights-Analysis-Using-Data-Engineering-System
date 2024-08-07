from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from snowflake.connector import connect
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'Admin',
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    dag_id='etl_local',
    description='Load data into Snowflake from Local Storage',
    schedule_interval=None,
    start_date=datetime(2024, 1, 29),
    default_args=default_args,
    catchup=False,
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
    local_csv_file = "data/hoteltariff.csv"
    with open(local_csv_file, 'r') as file:
        data = pd.read_csv(file)
    return data


def find_most_common(lst):
    count_dict = {} #{1500:2,1200:3}
    for item in lst:
        if item in count_dict:
            count_dict[item] += 1
        else:
            count_dict[item] = 1
    return max(count_dict, key=lambda x: count_dict[x])


def transform_data(extracted):
    data_frame = extracted.copy()

    tariff_columns = ['SundayTariff', 'MondayTariff', 'TuesdayTariff', 'WednesdayTariff', 'ThursdayTariff', 'FridayTariff', 'SaturdayTariff']
    weekend = ['FridayTariff', 'SaturdayTariff', 'SundayTariff']
    data_frame = data_frame.fillna(0)
    for index, row in data_frame.iterrows():
        row_values = row[tariff_columns]

        zero_count = 0
        non_zero_values = []
        for value in row_values:
            if value == 0:
                zero_count += 1
            elif value != 0:
                non_zero_values.append(value)

        modification_data_frame = data_frame.copy()
        if zero_count < len(row_values):
            for col in tariff_columns:
                if row[col] == 0:
                    if col in weekend:
                        max_value = max(non_zero_values)
                        modification_data_frame.at[index, col] = max_value
                    else:
                        most_repeated_value = find_most_common(non_zero_values)
                        modification_data_frame.at[index, col] = most_repeated_value

    return modification_data_frame




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
        snowflake_table = 'HotelTariff'
        columns = transformed_data.columns.tolist()

        drop_table_statement = f"DROP TABLE IF EXISTS {snowflake_table};"
        cur.execute(drop_table_statement)

        create_table_statement = f"CREATE TABLE {snowflake_table} ("
        for col in columns:
            if col in ['Hotel', 'City', 'District', 'Roomtype']:
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
            
            print(f"SQL statement: {sql_statement}")
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