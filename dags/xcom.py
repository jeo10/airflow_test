from airflow.sdk import dag, task, Context
from typing import Dict, Any

@dag
def xcom_dag():

    @task
    def t1() -> Dict[str, Any]:
        val = 42
        my_sentence = 'hello world'
        #context['ti'].xcom_push(key='my_key', value=val)
        return { # xcom_push(key='return_value', value=val)
            'val': val,
            'my_sentence': my_sentence
        }

    @task
    #def t2(context: Context):
    def t2(data: Dict[str, Any]):
        #val = context['ti'].xcom_pull(task_ids='t1', key='my_key')
        print(data['val'])
        print(data['my_sentence'])

    #t1() >> t2
    val = t1()
    t2(val)

xcom_dag()