# Telegram-RSS-Bot

A bot helps to subscribe to rss sources, and organise and post in channels/chats.

To use the bot, you need to start your own bot and build your own server. This readme will provide a ~~step-by-step~~ very concise guide.

## How To Start

- Create a telegram bot. Please refer to the official [guild](https://core.telegram.org/bots#3-how-do-i-create-a-bot). In short, talk to *[BotFather](https://t.me/botfather)* and say "/newbot".
- Start a server on Heroku. [Heroku](https://www.heroku.com/) is a cloud platform you can build apps on. It's free tier should be enough for your own use. There are definitely more options but this guide will only cover Heroku.
- Create an account [here](https://signup.heroku.com/), and [verify](https://www.heroku.com/verify) by adding a credit card to the account (for the bot to work, you will need some add-ons, and you have to verify with your credit card, but they won't charge you).
- Create an app.
- Add [Heroku Redis](https://elements.heroku.com/addons/heroku-redis) and [Heroku Scheduler](https://elements.heroku.com/addons/scheduler) to your app.
- Under "Deploy", choose a deployment method that suits you and follow the instructions.
- Under "Settings -> Config Vars", add a variable `WEBHOOK_TOKEN` (refer to [here](https://core.telegram.org/bots/api#setwebhook)). You can use a long, randomly generated string (recommended!), as you won't need it elsewhere.
- Similarly, add `BOT_TOKEN`, which should be found when you create your bot.
- Then, add `APP_NAME`, the name of your application.
- In *a Heroku console*, run the shell script `scripts/set_webhook.sh`. You can start a Heroku console with Heroku CLI or under "More -> Run console" on your app dashboard.
- In Telegram, add your bot to some groups/channels and promote the bot as an administrator. You should at least allow the bot to post messages.
- Check the chats information with `http://YOUR-APP.heroku.com/getChats` and prepare your `myconfig.yml`.
- Create a `myconfig.yml` similar to `simple_config.yml` (simple version) or `config.yml` (complete version) and run *locally* the shell script `scripts/update_config.sh`. ~~Alternatively, you can configure your bot via telegram~~ (TODO (maybe...))
- Add a job `python scripts/fetch.py` to your Heroku Scheduler

## APIs

- `/getChats`: Get information of all chats (groups, channels, private chats, ...) your bot can post messages to. Use the `chat_id` to configure your bot.
- `/getConfig`: Get the current configuration.
- `/setConfig`: Set a new configuration.
- `/updateHook`: Not for humans :). The bot's [update hook](https://core.telegram.org/bots/api#setwebhook). Any [updates](https://core.telegram.org/bots/api#getting-updates) will send a request to this interface.
