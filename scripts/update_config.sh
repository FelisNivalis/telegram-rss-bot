if [ -z "$1" ]; then
	echo "Usage: bash update_config.sh [Your config yaml file]"
	exit 1
fi
FILE_SIZE=$(du -bsh $1 | cut -f1)
if [[ $FILE_SIZE > 10000 ]]; then
	echo "Warning: Your config file \`$1\` is large ($FILE_SIZE bytes)."
fi
if [ -z "$HEROKU_APP_NAME" ]; then
	echo "Enter your app name:"
	read HEROKU_APP_NAME
fi
curl https://$HEROKU_APP_NAME.herokuapp.com/setConfig
echo ""
echo "Enter the verification Code:"
read VERIFICATION_CODE
curl -XPOST -F config.yml=@$1 https://$HEROKU_APP_NAME.herokuapp.com/setConfig/$VERIFICATION_CODE
