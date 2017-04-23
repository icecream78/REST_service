import sys
import os
import argparse
import psycopg2


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('createdb', default=False, help='Command for creation database')

    return parser


def _find_values(path, find_names, counter):
    with open(path, 'r') as f:
        for line in f:
            if counter == len(find_names):
                break

            command, value = map(str.strip, line.split('='))
            if command in find_names and value != '':
                find_names[command] = value[1:-1]
                counter += 1

    return find_names, counter


def get_credits():
    path = os.path.join(os.path.abspath(os.curdir), 'project_settings', 'settings_local.py')
    credit = {}
    try:
        with open(path, 'r') as f:
            for line in f:
                line = line.strip().split('=')
                key, value = map(str.strip, line)
                if "\'" in line or "\"" in value:
                    value = value[1:-1]
                credit[key] = value
        return credit
    except Exception:
        return {}


def create_db():
    data = get_credits()

    try:
        connect = psycopg2.connect(database='data_saver',
                                   user=data['POSTGRES_USER'],
                                   host=data['POSTGRES_HOST'],
                                   password=data['POSTGRES_PASSWORD'],
                                   port=data['POSTGRES_PORT']
                                   )
        cursor = connect.cursor()
        cursor.execute('DROP TABLE IF EXISTS saved_data')
        cursor.execute('CREATE SEQUENCE data_ids')
        cursor.execute(
            'CREATE TABLE saved_data(data_id INTEGER PRIMARY KEY DEFAULT NEXTVAL(\'data_ids\'), user_id INTEGER, data TEXT)')
        connect.commit()
        connect.close()
        return True
    except Exception:
        return False


if __name__ == '__main__':
    parser = create_parser()
    create_flag = parser.parse_args(sys.argv[1:])

    if create_flag:
        if create_db():
            print('database created')
        else:
            print('error while creating database')
