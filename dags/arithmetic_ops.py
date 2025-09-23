'''
Task 1: To start with a number (say 100)
Task 2: To add 50 to the number
Task 3: To multiply the result by 2
Task 4: To devide the result by 10 
'''
#create a dag
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

#define the function for each task 

def start_number(**context):
    context ["ti"].xcom_push(key = 'current_value',value=100)
    print ("Starting number is 100")

def add_fifty (**context):
    current_value = context['ti'].xcom_pull(key = 'current_value', task_ids = 'start_number') #get the 'current_value' from previous task
    new_value = current_value + 50 #create new value
    context ["ti"].xcom_push(key = 'current_value',value = new_value) #push (update) the new value
    print (f'add 50: {current_value}+50 = {new_value}')

def multiply_two(**context):
    current_value = context['ti'].xcom_pull(key = 'current_value', task_ids = 'add_fifty')
    new_value = current_value * 2 
    context ["ti"].xcom_push(key = 'current_value',value = new_value)
    print (f'multiply by 2: {current_value}*2 = {new_value}')

def devide_ten(**context):
    current_value = context['ti'].xcom_pull(key = 'current_value', task_ids = 'multiply_two')
    new_value = current_value / 10
    context ["ti"].xcom_push(key = 'current_value',value = new_value)
    print (f'devide by 10: {current_value}/10 = {new_value}')

#define the dag 
with DAG (
    dag_id = 'Arithmetic_opertations' #this is one of the task
) as dag: #task 1
    start_number = PythonOperator(
        task_id = "start_number", #task 1 name
        python_callable = start_number, #python_callable is to call the value as a function
        #provide_context = True #dont go out of the context, context allow to pass values across tasks
    )
    
    add_fifty = PythonOperator(
        task_id="add_fifty",
        python_callable = add_fifty,
        #provide_context = True
    )

    multiply_two = PythonOperator(
        task_id="multiply_two",
        python_callable = multiply_two,
        #provide_context = True
    )

    devide_ten = PythonOperator(
        task_id="devide_ten",
        python_callable = devide_ten,
        #provide_context = True
    )

    #dependencies
    start_number >> add_fifty >> multiply_two >> devide_ten