import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
import config_source
import psycopg2

# Параметры, с какого времени запускать DAG
args = {
    'owner': 'Valentin',
    'start_date': datetime.datetime(2023, 2, 5),
    'provide_context': True
}


def extract_data(**kwargs):
    ti = kwargs['ti']
    conn = config_source.conf()
    cursor = conn.cursor()

    list_table = ['course', 'stream',  'stream_module_lesson']
    info = {}

    for table in list_table:
        with cursor as curs:
            curs.execute(f"SELECT * FROM {table}")
            rows = curs.fetchall()
            info[table] = (rows)
    with cursor as curs:
        curs.execute(f"select Title, created_at, updated, order_in_stream, deleted, course_id from (SELECT *  FROM stream_module Join stream on stream.id = stream_module.stream_id Join course on course.id = stream.course_id) as Module group by Title; ALTER TABLE Module ADD id int identity;")
        rows = curs.fetchall()
        info['module'] = (rows)

    ti.xcom_push(key='sourse_info', value=info)
    return info

# catchup - запускать даг с времени, указанного в args.
if __name__ == '__main__':
    with DAG('load_weather_wwo', description='load_info_from_source_to_DWH', catchup=False, default_args=args) as dag:  # 0 * * * *   */1 * * * *
        extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data)
        create_course_table = PostgresOperator(
            task_id="create_course_table",
            postgres_conn_id="database",
            sql="""create table course(
                    id                    integer      primary key,
                    title                 varchar(255),
                    created_at            timestamp,
                    updated_at            timestamp,
                    deleted_at            timestamp(0),
                    icon_url              varchar(255),
                    is_auto_course_enroll boolean,
                    is_demo_enroll        boolean);""",
)
        insert_in_table_course = PostgresOperator(
            task_id="insert_clouds_table",
            postgres_conn_id="database",
            sql=[f"""INSERT INTO course VALUES(
                         '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{course}]['id']}}}}',
                        '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{course}]['title']}}}}',
                        '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{course}]['created_at']}}}}',
                        '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{course}]['updated_at']}}}}',
                        '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{course}]['deleted_at']}}}}',
                        '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{course}]['icon_url']}}}}',
                        '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{course}]['is_auto_course_enroll']}}}}',
                        '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{course}]['is_demo_enroll']}}}}')
                        """ for i in range(len(extract_data['course']))]
        )
        create_stream_table = PostgresOperator(
            task_id="create_stream_table",
            postgres_conn_id="database",
            sql="""create table stream(
                    id                     integer primary key,
                    course_id              integer,
                    start_at               timestamp,
                    end_at                 timestamp,
                    created_at             timestamp,
                    updated_at             timestamp,
                    deleted_at             timestamp,
                    is_open                boolean,
                    name                   varchar(255),
                    homework_deadline_days integer);""",
)
        insert_in_table_stream = PostgresOperator(
            task_id="insert_stream_table",
            postgres_conn_id="database",
            sql=[f"""INSERT INTO stream VALUES(
                     '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{stream}]['id']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{stream}]['course_id']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{stream}]['start_at']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{stream}]['end_at']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{stream}]['created_at']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{stream}]['updated_at']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{stream}]['deleted_at']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{stream}]['is_open']}}}}'),
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{stream}]['name']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{stream}]['homework_deadline_days']}}}}')
                    """ for i in range(len(extract_data['stream']))]
        )
        create_module_table = PostgresOperator(
            task_id="create_module_table",
            postgres_conn_id="database",
            sql="""create table module(
                    title           varchar(255),
                    created_at      timestamp,
                    updated_at      timestamp,
                    order_in_stream integer,
                    deleted_at      timestamp,
                    course_id       integer,
                    id  integer primary key);""",
)
        insert_in_table_module = PostgresOperator(
            task_id="insert_module_table",
            postgres_conn_id="database",
            sql=[f"""INSERT INTO module VALUES(
                     '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module}]['title']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module}]['created_at']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module}]['updated_at']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module}]['order_in_stream']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module}]['deleted_at']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module}]['course_id']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module}]['id']}}}}'
                    """ for i in range(len(extract_data['module']))]
        )
        create_module_lesson_table = PostgresOperator(
            task_id="create_module_lesson_table",
            postgres_conn_id="database",
            sql="""create table module_lesson(
                    id                          integer primary key,
                    title                       varchar(255),
                    description                 text,
                    start_at                    timestamp,
                    end_at                      timestamp,
                    homework_url                varchar(500),
                    teacher_id                  integer,
                    stream_module_id            integer,
                    deleted_at                  timestamp(0),
                    online_lesson_join_url      varchar(255),
                    online_lesson_recording_url varchar(255));""",
)
        insert_in_table_module_lesson = PostgresOperator(
            task_id="insert_module_lesson_table",
            postgres_conn_id="database",
            sql=[f"""INSERT INTO module_lesson VALUES(
                     '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module_lesson}]['id']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module_lesson}]['title']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module_lesson}]['description']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module_lesson}]['start_at']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module_lesson}]['end_at']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module_lesson}]['homework_url']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module_lesson}]['teacher_id']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module_lesson}]['stream_module_id']}}}}'),
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module_lesson}]['deleted_at']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module_lesson}]['online_lesson_join_url']}}}}',
                    '{{{{ti.xcom_pull(key='sourse_info', task_ids=['extract_data'])[0].iloc[{module_lesson}]['online_lesson_recording_url']}}}}')
                    """ for i in range(len(extract_data['module_lesson']))]
        )

        extract_data >> create_course_table >> insert_in_table_course >> create_stream_table >> insert_in_table_stream >> create_module_table >> insert_in_table_module >> create_module_lesson_table >> insert_in_table_module_lesson


