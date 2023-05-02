import psycopg2

def conf():
    try:
        # пытаемся подключиться к базе данных
        conn = psycopg2.connect(dbname='sorce',
                                user='user',
                                password='password',
                                host='host')
        return conn

    except:
        # в случае сбоя подключения будет выведено сообщение об ошибке
        print('Can`t establish connection to database')

if __name__ == '__main__':
    conf()