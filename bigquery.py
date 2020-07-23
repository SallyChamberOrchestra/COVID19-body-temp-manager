import json
import hashlib
import os
from google.cloud import bigquery


def create_anonymized_name(name):
    # TODO: 暫定
    return hashlib.md5(name.encode()).hexdigest()


class BigQueryHandler():
    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT')
        self.dataset_name = 'body_temperature_data'
        self.bq = bigquery.Client()
        self.dataset = self.bq.dataset(self.dataset_name)

    def insert(self, data):
        # extract data
        user_id = data.get('user_id')
        user_name = data.get('user_name')
        body_temp = data.get('body_temp')
        datetime = data.get('datetime')

        res_user = self._insert_user_if_not_exists(user_id, user_name)
        res_temp = self._insert_temperature(datetime, user_id, body_temp)

        return {
            'user_insertion_result': res_user,
            'temperature_insertion_result': res_temp
        }

    def _insert_user_if_not_exists(self, user_id, user_name):
        # table = self.dataset.table('user')
        table = self.bq.get_table(
            f'{self.project_id}.{self.dataset_name}.user')

        # check if the user_id already exists
        q = (
            f'SELECT id FROM {self.project_id}.{self.dataset_name}.user WHERE id = "{user_id}"'
        )
        rows = self.bq.query(q).result()
        if rows.total_rows > 0:
            # already exists
            return {'created': False, 'user_data': rows[0]}

        # insert new user
        data = {'name': user_name, 'id': user_id,
                'anonymized_name': create_anonymized_name(user_name)}
        self._insert_to_bq(table, data)
        return {'created': True, 'user_data': data}

    def _insert_temperature(self, datetime, user_id, body_temp):
        # table = self.dataset.table('temperature')
        table = self.bq.get_table(
            f'{self.project_id}.{self.dataset_name}.temperature')
        datetime_str = datetime.strftime('%Y-%m-%dT%H:%M:%S')
        data = {'datetime': datetime_str,
                'user_id': user_id, 'temperature': body_temp}
        self._insert_to_bq(table, data)

        return {'duplicates': False, 'body_temp_data': data}
        # check duplicates on the same date
        # q = (
        #     f'SELECT user_id FROM {self.project_id}.{self.dataset_name}.temperature WHERE datetime = {datetime}'
        # )
        # rows = self.bq.query(q).result()
        # if len(rows) > 1:
        #     # duplicates
        #     return {'duplicates': True, 'body_temp_data': data}
        # else:
        #     return {'duplicates': False, 'body_temp_data': data}

    def _insert_to_bq(self, table, data):
        errors = self.bq.insert_rows(table, [data])

        if errors != []:
            raise BigQueryError(errors)


class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened'''

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)
