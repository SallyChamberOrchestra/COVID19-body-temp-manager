import json
import logging
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
        table = self.bq.get_table(
            f'{self.project_id}.{self.dataset_name}.user')

        # check if the user_id already exists
        q = (
            f'SELECT id, name, anonymized_name FROM {self.project_id}.{self.dataset_name}.user '
            f'WHERE id = "{user_id}"'
        )
        rows = self.bq.query(q).result()
        if rows.total_rows > 0:
            # already exists
            df = rows.to_dataframe(self.bq)
            logging.info(df)
            logging.info(df.to_dict())
            logging.info(df.iloc[0, :].to_dict())
            return {'created': False, 'user_data': df.iloc[0, :].to_dict()}

        # insert new user
        data = {'name': user_name, 'id': user_id,
                'anonymized_name': create_anonymized_name(user_name)}
        self._insert_to_bq(table, data)
        return {'created': True, 'user_data': data}

    def _insert_temperature(self, datetime, user_id, body_temp):
        table = self.bq.get_table(
            f'{self.project_id}.{self.dataset_name}.temperature')
        datetime_str = datetime.strftime('%Y-%m-%dT%H:%M:%S')
        data = {'datetime': datetime_str,
                'user_id': user_id, 'temperature': body_temp}
        self._insert_to_bq(table, data)

        # check duplicates on the same date
        q = (
            f'WITH converted_date AS ('
            f'  SELECT'
            f'    DATE(TIMESTAMP(datetime), "Asia/Tokyo") as register_date,'
            f'    CURRENT_DATE("Asia/Tokyo") as today,'
            f'  FROM {self.project_id}.{self.dataset_name}.temperature'
            f')'
            f'SELECT * FROM converted_date WHERE register_date=today'
        )
        rows = self.bq.query(q).result()
        if rows.total_rows > 0:
            # duplicates
            return {'duplicates': True, 'body_temp_data': data}
        else:
            return {'duplicates': False, 'body_temp_data': data}

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
