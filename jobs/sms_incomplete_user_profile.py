# Send incomplete profile reminder SMSes to monthly parking customers

import os, requests, logging
from dotenv import load_dotenv
from sqlalchemy import text, create_engine
from datetime import datetime

load_dotenv()

db_uri = os.getenv('DB_URI')

# engine = create_engine(db_uri, echo=True) #echo=True for debugging
engine = create_engine(db_uri)
db = engine.connect()

carpark_id = os.getenv('CARPARK_ID')  # only send to Tai Po monthly customers for now
message_id = 1  # message id for incomplete profile sms

# get the content for incomplete profile SMS
sql = text("""
    SELECT content from message where id = :message_id
""").bindparams(message_id=message_id)

cursor = db.engine.execute(sql)
msg = cursor.fetchone()

now = datetime.today()
this_morning = datetime.strftime(now, '%Y-%m-%d 5:00:00')

# get all users with incomplete profile
sql = text("""
    SELECT mobile.number, mobile.user_id 
    FROM mobile
    INNER JOIN user ON user.id = mobile.user_id AND user.date_created <= :this_morning AND user.date_deleted IS NULL
    LEFT JOIN user_message_log ON user.id = user_message_log.user_id AND user_message_log.message_id = :message_id
    LEFT JOIN octopus ON user.id = octopus.user_id 
    LEFT JOIN vehicle ON user.id = vehicle.user_id
    INNER JOIN user_carpark_rental ON user_carpark_rental.carpark_id = :carpark_id AND user.id = user_carpark_rental.user_id
    WHERE octopus.id IS NULL AND vehicle.id IS NULL AND user_message_log.id IS NULL AND mobile.is_verified = 1 AND mobile.date_deleted IS NULL;
""").bindparams(this_morning=this_morning, carpark_id=carpark_id, message_id=message_id)

cursor = db.engine.execute(sql)
results = cursor.fetchall()

sms_count = 0

# send incomplete profile SMS reminder to these users
for result in results:
    country_code = "852"
    mobile_no = result.number
    content = msg['content']
    # TODO: replace content variables (carpark name, url_name) with actual values

    params = {
        "apiusername": os.getenv('ONE_WAY_SMS_API_USERNAME'),
        "apipassword": os.getenv('ONE_WAY_SMS_API_PASSWORD'),
        "senderid": "Uvision",
        "languagetype": 2,
        "mobileno": f"{country_code}{mobile_no}",
        "message": content
    }

    print(params)

    response = requests.get("https://sgateway.onewaysms.com/apichinese20.aspx", params=params)

    print(response.content.decode("utf-8"))

    if response.content.decode("utf-8") == "-100":  # payment required
        logging.error("One Way SMS API login and password incorrect")
        raise SystemExit("One Way SMS API login and password incorrect")

    elif response.content.decode("utf-8") == "-300":
        logging.error("One Way SMS API responded with invalid mobile number: " + str(country_code) + str(mobile_no))
        continue

    elif response.content.decode("utf-8") == "-400" or str(response.content) == "-500":
        logging.error("One Way SMS API call returned the following error code:" + response.content.decode(
            "utf-8") + ", content: " + content)
        raise SystemExit("One Way SMS API call returned the following error code:" + response.content.decode(
            "utf-8") + ", content: " + content)

    elif response.content.decode("utf-8") == "-600":
        logging.error("One Way SMS API balance needs to be topped up")
        raise SystemExit("One Way SMS API balance needs to be topped up")

    sql = text("""
        INSERT INTO user_message_log (user_id, message_id, content) 
        VALUES (:user_id, :message_id, :content)
    """).bindparams(user_id=result.user_id, message_id=message_id, content=content)

    db.engine.execute(sql)

    sms_count += 1

if results:
    print(f"{sms_count} SMSes sent")
else:
    print("No new users found")
