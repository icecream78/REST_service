from flask import Flask, jsonify, abort, make_response, request
import os
import psycopg2
import json
from celery import Celery

app = Flask(__name__)
app.config.from_object('project_settings.settings')
settings_file = os.path.join(app.root_path, 'project_settings', 'settings_local.py')
app.config.from_pyfile(settings_file, silent=True)
app.config['CELERY_BROKER_URL'] = 'redis://127.0.0.1:6300/0'


def make_celery(app):
    celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery
celery = make_celery(app)


def json_worker(data):
    data = map(lambda d: d[0], data)
    concat = '[{}]'.format(','.join(data))
    return concat


# work with db
def connect_db():
    return psycopg2.connect(database='data_saver',
                            user=app.config['POSTGRES_USER'],
                            host=app.config['POSTGRES_HOST'],
                            password=app.config['POSTGRES_PASSWORD'],
                            port=app.config['POSTGRES_PORT']
                            )


def connector(query, *args):
    sql = connect_db()
    cur = sql.cursor()
    cur.execute(query, args)

    try:
        res = cur.fetchall()
    except psycopg2.ProgrammingError:
        res = None
    finally:
        sql.commit()
        sql.close()

    return res


def get_db_data(user_id):
    q = connector('SELECT data FROM saved_data WHERE user_id = %s', user_id)
    data = json_worker(q)
    # sql.close()
    if len(data) > 0:
        return data
    return ''


def put_db_data(user_id, data):
    data = json.dumps(data)
    r = connector('INSERT INTO saved_data (user_id, data) VALUES (%s,%s)', user_id, data)
    if r is None:
        return True
    else:
        return False


@celery.task
def delete_db_data(user_id):
    q = connector('SELECT COUNT(*) FROM saved_data WHERE user_id = %s', user_id)
    if len(q[0]) > 0:
        connector('DELETE FROM saved_data WHERE user_id = %s', user_id)
    print('User {} was deleted.'.format(user_id))

# end work with db


@app.errorhandler(404)  # making custom error response
def not_found(error):
    response = make_response(jsonify({'error': error.description}), 400)
    response.headers['Error Type'] = 'Not found'
    return response


@app.route('/api/<int:user_id>', methods=['GET'])
def get_data(user_id):
    data = get_db_data(user_id)
    if len(data) > 0:
        return jsonify({'data': json.loads(data)})
    else:
        abort(404, 'No data or given user wasn`t found')


@app.route('/api/<int:user_id>', methods=['POST'])
def put_data(user_id):
    if len(request.json) == 0:
        abort(404, 'POST request don`t contain any data')

    if put_db_data(user_id, request.json):
        delete_db_data.apply_async(args=[user_id], countdown=60*60)
        return jsonify({'result': 'Data was insert to db'})
    else:
        abort(404, 'Can`t insert provided data to database. Please, check it and try again')


if __name__ == '__main__':
    app.run('127.0.0.1', 8000)
