from datetime import datetime
import os
import base64
import hashlib
import hmac
import logging
import requests
import traceback

from flask import abort, jsonify

from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

from validator import BodyTemperatureValidator
from bigquery import BigQueryHandler, BigQueryError, create_anonymized_name

DASHBOARD_URL = os.environ.get('DASHBOARD_URL')


def register_temperature(request):
    execution_id = request.headers.get('Function-Execution-Id')
    channel_secret = os.environ.get('LINE_CHANNEL_SECRET')
    channel_access_token = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')

    bot_api = LineBotApi(channel_access_token)
    parser = WebhookParser(channel_secret)

    body = request.get_data(as_text=True)
    hash = hmac.new(channel_secret.encode('utf-8'),
                    body.encode('utf-8'), hashlib.sha256).digest()
    signature = base64.b64encode(hash).decode()

    if signature != request.headers['X_LINE_SIGNATURE']:
        return abort(405)

    try:
        events = parser.parse(body, signature)
    except InvalidSignatureError:
        return abort(405)

    for event in events:
        if not isinstance(event, MessageEvent):
            continue
        if not isinstance(event.message, TextMessage):
            continue

        handle_event(event, channel_access_token, bot_api, execution_id)
    return jsonify({'message': 'ok'})


def handle_event(event, token, bot_api, execution_id):
    replier = MessageReplier(bot_api, event.reply_token)
    # fetch user id & user name
    user_id, user_name = get_user_info(event, token)
    # message
    message = event.message.text
    # parse & validation
    validator = BodyTemperatureValidator()
    try:
        body_temp = validator.parse_and_validate(message)
    except ValueError as e:
        # validation error
        replier.reply(str(e))
        return abort(405)

    # register to DB
    bq = BigQueryHandler()

    data = {
        'user_id': user_id,
        'user_name': user_name,
        'body_temp': body_temp,
        'datetime': datetime.now()
    }
    try:
        result = bq.insert(data)
        # post process
        reply_by_result(result, replier)
    except BigQueryError as e:
        replier.reply_with_error(
            '登録中にエラーが発生しました。運営まで一報ください。', 'E001', execution_id)
        logging.error(str(e))
        logging.info(traceback.format_exc())
        return abort(500)
    except Exception as e:
        replier.reply_with_error(
            '登録中にエラーが発生しました。運営まで一報ください。', 'E002', execution_id)
        logging.error(str(e))
        logging.info(traceback.format_exc())
        return abort(500)


def get_user_info(event, token):
    user_id = event.source.user_id

    headers = {'Authorization': f'Bearer {token}'}
    res = requests.get(
        f'https://api.line.me/v2/bot/profile/{user_id}', headers=headers)
    data = res.json()
    user_name = data['displayName']

    return user_id, user_name


def reply_by_result(result, replier):
    res_user = result['user_insertion_result']
    res_temp = result['temperature_insertion_result']
    user_name = res_user['user_data']['name']

    if res_user['created']:
        msg = f"{user_name}さん、こんにちは。初回の体温を登録しました。\n"
    elif res_temp['duplicates']:
        msg = '本日分の体温記録を更新しました。\n'
    else:
        msg = '本日分の体温を記録しました。\n'
    msg += f'{user_name}さんの体温推移：{create_url(user_name)}'

    replier.reply(msg)


def create_url(name):
    id_ = create_anonymized_name(name)
    url = DASHBOARD_URL + f'?____=14&id={id_}#hide_parameters=id'
    return url


class MessageReplier():
    def __init__(self, bot_api, reply_token):
        self.bot_api = bot_api
        self.reply_token = reply_token

    def reply(self, message):
        self.bot_api.reply_message(
            self.reply_token,
            TextSendMessage(text=message)
        )

    def reply_with_error(self, message, error_code, execution_id):
        message += f'\r\nエラーコード: {error_code}'
        message += f'\r\n実行ID: {execution_id}'
        self.reply(message)
