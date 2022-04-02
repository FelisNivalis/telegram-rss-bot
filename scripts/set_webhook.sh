curl "https://api.telegram.org/bot$BOT_TOKEN/setWebhook?url=https://$HEROKU_APP_NAME.herokuapp.com/updateHook/$WEBHOOK_TOKEN&allowed_updates=[\"my_chat_member\"]"
