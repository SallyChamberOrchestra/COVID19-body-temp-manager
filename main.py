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
from bigquery import BigQueryHandler, BigQueryError


def register_temperature(request):
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

        handle_event(event, channel_access_token, bot_api)
    return jsonify({'message': 'ok'})


def handle_event(event, token, bot_api):
    user_id, user_name = get_user_info(event, token)
    # message
    message = event.message.text
    # parse & validation
    validator = BodyTemperatureValidator()
    try:
        body_temp = validator.parse_and_validate(message)
    except ValueError as e:
        # validation error
        bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=str(e))
        )
        return

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
        reply_by_result(result, event.reply_token, bot_api)
    except BigQueryError as e:
        bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text='登録中にエラーが発生しました。運営まで一報ください。エラーコード:E001')
        )
        logging.error(str(e))
        logging.info(traceback.format_exc())
    except Exception as e:
        logging.error(str(e))
        logging.info(traceback.format_exc())
        bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text='登録中にエラーが発生しました。運営まで一報ください。エラーコード:E002')
        )


def get_user_info(event, token):
    user_id = event.source.user_id

    headers = {'Authorization': f'Bearer {token}'}
    res = requests.get(
        f'https://api.line.me/v2/bot/profile/{user_id}', headers=headers)
    data = res.json()
    user_name = data['displayName']

    return user_id, user_name


def reply_by_result(result, reply_token, bot_api):
    res_user = result['user_insertion_result']
    res_temp = result['temperature_insertion_result']

    if res_user['created']:
        bot_api.reply_message(
            reply_token,
            TextSendMessage(
                text=f"{res_user['user_data']['name']}さん、こんにちは。初回の体温を登録しました。")
        )
    elif res_temp['duplicates']:
        bot_api.reply_message(
            reply_token,
            TextSendMessage(text='本日分の体温記録を更新しました。')
        )
    else:
        bot_api.reply_message(
            reply_token,
            TextSendMessage(text='本日分の体温を記録しました。')
        )
