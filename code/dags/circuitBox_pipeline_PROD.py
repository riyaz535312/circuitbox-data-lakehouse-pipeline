from airflow.sdk import dag,task,get_current_context
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from airflow.providers.smtp.operators.smtp import EmailOperator
from datetime import datetime,timezone
from airflow.exceptions import AirflowFailException

@dag(
    dag_id ="circuitBox_project_PROD",
    schedule = None,
    catchup=False
)
def circuitbox_dag_prod():

    # task to check the source file in S3 bucket
    check_s3_files = S3KeySensor(
        task_id="check_s3_files",
        bucket_key=['source_files/addresses/addresses_*.csv',
                    'source_files/orders/orders_*.json',
                    'source_files/customers/customers_*.json'],
        bucket_name='circuitboxproject',
        wildcard_match=True, # for * at the end of folder/file_name 
        poke_interval=30,
        timeout=100,
        aws_conn_id='aws_conn',
        soft_fail=False, # used to skip not fail after files not found
        mode="reschedule" # release the worker after every poke 
    )

    # task to copy the files from S3 to ADLS using Hooks
    @task
    def copy_s3_to_adls(**context):
        
        flag= False # to validate weather files are copied or not

        #last_run_time= context['data_interval_start'] # to fetch last dag runtime
        last_run_time=datetime(2026, 2, 10, 0, 0, 0, tzinfo=timezone.utc)

        s3_hook=S3Hook(aws_conn_id='aws_conn')
        adls_hook =AzureDataLakeStorageV2Hook( adls_conn_id='wasb_conn')


        s3_py_api= s3_hook.get_conn()
        # to get the boto3 object( python api for S3)

        bucket='circuitboxproject'
        prefix='source_files/'

        response=s3_py_api.list_objects_v2( # to retrive upto 1,000 objects from s3 bucket in dict format
            Bucket=bucket,
            Prefix=prefix
        )

        objects=response.get('Contents',[]) # fetch only Contents data from response dict
        
        for obj in objects:

            key=obj['Key']
            last_modified=obj['LastModified']
            if key.endswith("/"):
                continue
            
            if last_modified > last_run_time:
                file_obj= s3_py_api.get_object(
                    Bucket=bucket,
                    Key=key
                )
                # we are using this to stream data from s3 to adls because a file might have more data 
                data=file_obj['Body'].read()

                new_path = f"landing/operational_data/{key.replace('source_files/','')}"
                
                file_sys_client=adls_hook.get_conn().get_file_system_client(
                    file_system='circuitbox'
                )
                file_client=file_sys_client.get_file_client(new_path)

                file_client.upload_data(
                    data,
                    overwrite=True
                )

                archive_path=key.replace('source_files/','archive_files/')

                s3_hook.copy_object(
                    source_bucket_key=key,
                    dest_bucket_key=archive_path,
                    source_bucket_name=bucket,
                    dest_bucket_name=bucket
                )

                s3_hook.delete_objects(
                    bucket=bucket,
                    keys=key
                )

                flag=True
        
        if not flag:
            raise AirflowFailException("Files are not copied to ADLS. please check!")
    
    
    # task to send mail if files no found
    Email_notify= EmailOperator(
        task_id='Email_notify', # used to call this task from another task 
        to='skriyaz535312@gmail.com',
        subject ="Source Files not found",
        html_content="""
                        <h2> Source files are not there in S3 bucket
                        <p> Please check and place the files in the path 
                        <p> or place the latest files in the path
                    """,
        trigger_rule="one_failed"
    )
    
    # task to tigger databricks pipeline 
    trigger_dlt= DatabricksRunNowOperator(
        task_id="Databricks_pipeline",
        job_id=242538008180628,
        databricks_conn_id ="databricks_conn"
        #,trigger_rule="on_skipped"
    )

    copy_task=copy_s3_to_adls()

    check_s3_files >> copy_task >> trigger_dlt

    [check_s3_files,copy_task] >>Email_notify
    


circuitbox_dag_prod()