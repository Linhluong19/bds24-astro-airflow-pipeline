"""
Task flow api allow us to use decorators instead of operators 
such as PythonOperator

Task 1: To start with a number (say 100)
Task 2: To add 50 to the number
Task 3: To multiply the result by 2
Task 4: To devide the result by 10 
"""
#operator cannot give data to the next task => we have to use push and pull
#decorator

#create a dag
from airflow import DAG
from airflow.decorators import task 
from datetime import datetime

#define a dag
with DAG(
    dag_id='arithmetic_operator_tfapi'
) as dag:
    
    #Task 1: Start with a number (say 100)
    @task
    def start_number():
        initial_value=100
        print (f'stating_number: {initial_value}')
        return initial_value
   
   #Task 2: To add 50 to the number
    @task
    def add_fifty(number):
       new_value = number + 50
       print (f'Add fifty: {number} + 50 = {new_value}')
       return new_value
   
   #Task 3: To multiply the result by 2
    @task
    def multiply_two(number):
       new_value = number * 2
       print (f'Multiply by two: {number} * 2 = {new_value}')
       return new_value

   #Task 4: To devide the result by 10
    @task
    def devide_ten(number):
       new_value = number / 10
       print (f'Devide by ten: {number} / 10 = {new_value}')
       return new_value 
    

    #dependencies is part of the dag, pay attention to the tab
    start_value = start_number() #start_value returned by what task
    second_value = add_fifty(start_value)
    third_value = multiply_two(second_value)
    fourth_value = devide_ten(third_value)