# Send payment reminder SMSes to monthly parking customers to ask them to pay up
import os, requests, logging, calendar, string, secrets, traceback

from dotenv import load_dotenv
from sqlalchemy import text, create_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta
from copy import copy

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from python_http_client.exceptions import HTTPError

PAYMENT_METHOD_ONLINE = 1
PAYMENT_METHOD_OCTOPUS = 2

PAYMENT_STATUS_PAID = 2

db = None
remaining_days = None
today = None


def send_sms(params):
    # Send SMS thru API gateway
    response = requests.get("https://sgateway.onewaysms.com/apichinese20.aspx", params=params)

    if response.content.decode("utf-8") == "-100":  # payment required
        logging.error("One Way SMS API login and password incorrect")
        raise SystemExit("One Way SMS API login and password incorrect")

    elif response.content.decode("utf-8") == "-300":
        logging.error(
            "One Way SMS API responded with invalid mobile number: " + str(params['mobileno']))

    elif response.content.decode("utf-8") == "-400" or str(response.content) == "-500":
        logging.error("One Way SMS API call returned the following error code:" + response.content.decode(
            "utf-8") + ", content: " + params['message'])
        raise SystemExit("One Way SMS API call returned the following error code:" + response.content.decode(
            "utf-8") + ", content: " + params['message'])

    elif response.content.decode("utf-8") == "-600":
        logging.error("One Way SMS API balance needs to be topped up")
        raise SystemExit("One Way SMS API balance needs to be topped up")

    elif int(response.content.decode("utf-8")) < 0:
        logging.error(f"One Way SMS API returned error code: {response.content.decode('utf-8')}")

    print("REAL SMS delivered to: ", params['mobileno'], " content: ", params['message'])


def send_email(to, subject, content):
    to_email = to
    if os.getenv('DEV'):
        to_email = os.getenv('STAFF_EMAIL')

    message = Mail(
        from_email="UPark HQ系統 <no-reply@uvision.ai>",
        to_emails=to_email,
        subject=subject,
        html_content=content
    )

    # MUST USE bugmagnet@gmail.com as bcc, because if use phil@uvision.ai - ZOHO mail server rejects it
    # message.add_bcc('bugmagnet@gmail.com')

    try:
        # print("sendgrid: ", os.getenv('SENDGRID_API_KEY'))
        sgrid = SendGridAPIClient(os.getenv('SENDGRID_API_KEY'))
        response = sgrid.send(message)

        # print(response.status_code)
        logging.debug(f"Response status code: {response.status_code}")
        return True

    except HTTPError as e:
        logging.error(e.to_dict)

    except Exception as e:
        print(traceback.format_exc())
        logging.error(
            f"Failed to send email to {os.getenv('STAFF_EMAIL')}, content: {content}, error: {e}")
        return False


def get_last_day_of_month(date_obj):
    month = datetime.strftime(date_obj, '%m')
    year = datetime.strftime(date_obj, '%Y')
    day = calendar.monthrange(int(year), int(month))[1]

    return datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d').date()


def get_supported_carparks():
    exclude_carpark_sql = None
    if os.getenv('EXCLUDED_CARPARK_IDS') is not None and os.getenv('EXCLUDED_CARPARK_IDS') != "":
        excluded_carpark_ids = os.getenv('EXCLUDED_CARPARK_IDS').split(",")
        exclude_carpark_sql = " AND carpark_id NOT IN :excluded_carpark_ids"
        print(f"SQL excluded carparks: {excluded_carpark_ids}")

    if not exclude_carpark_sql:
        sql = text(f"""
                SELECT carpark_config.carpark_id FROM carpark_config
                WHERE (accept_online_payment = 1 OR accept_octopus_payment = 1)
                    AND carpark_config.enable_monthly_rental = 1
            """)
    else:
        sql = text(f"""
                        SELECT carpark_config.carpark_id FROM carpark_config
                        WHERE (accept_online_payment = 1 OR accept_octopus_payment = 1)
                            AND carpark_config.enable_monthly_rental = 1
                            {exclude_carpark_sql}
                    """).bindparams(excluded_carpark_ids=excluded_carpark_ids)

    cursor = db.engine.execute(sql)
    results = cursor.fetchall()
    carpark_ids = []

    for r in results:
        carpark_ids.append(r.carpark_id)

    return carpark_ids



def get_unpaid_users(carpark_ids, date_var=datetime.today()):
    this_month_start = (date_var.replace(day=1)).date()
    this_month_end = get_last_day_of_month(this_month_start)

    # first get all users who have paid for this month
    sql = text("""
        SELECT user_carpark_rental.user_id, user_carpark_rental.id as rental_id, 
        user_carpark_rental.carpark_id, carpark.name as carpark_name, start_date 
        FROM user_carpark_rental
        INNER JOIN carpark ON carpark.id = user_carpark_rental.carpark_id
        INNER JOIN payment ON payment.rental_id = user_carpark_rental.id AND payment.status = :payment_status
        WHERE user_carpark_rental.carpark_id IN :carpark_ids AND start_date >= :month_start AND end_date <= :month_end
    """).bindparams(
        payment_status=PAYMENT_STATUS_PAID,
        carpark_ids=carpark_ids,
        month_start=this_month_start,
        month_end=this_month_end
    )

    paid_users = db.engine.execute(sql).fetchall()

    paid_user_ids = []
    for u in paid_users:
        paid_user_ids.append(u.user_id)

    next_month_start = this_month_start + relativedelta(months=1)
    next_month_end = get_last_day_of_month(next_month_start)

    next_paid_user_ids = []
    if paid_user_ids:
        # next filter out all the above users and get those who have paid next month's rent as well
        sql = text("""
            SELECT user_carpark_rental.user_id, user_carpark_rental.id as rental_id
                FROM user_carpark_rental 
            INNER JOIN payment ON payment.rental_id = user_carpark_rental.id AND payment.status = :payment_status 
            WHERE user_carpark_rental.user_id IN :paid_user_ids AND start_date >= :month_start AND end_date <= :month_end
        """).bindparams(
            payment_status=PAYMENT_STATUS_PAID,
            paid_user_ids=paid_user_ids,
            month_start=next_month_start,
            month_end=next_month_end
        )
        next_paid_users = db.engine.execute(sql).fetchall()

        for u in next_paid_users:
            next_paid_user_ids.append(u.user_id)

    # then loop thru this month's paid user list and remove all users who have paid next month's rent (keeping only users who paid this month's rent but not next month's)
    unpaid_users = copy(paid_users)
    for u in paid_users:
        if u.user_id in next_paid_user_ids:
            unpaid_users.remove(u)

    return unpaid_users


def send_payment_reminder():
    global db, remaining_days, today

    db_uri = os.getenv('DB_URI')

    # engine = create_engine(db_uri, echo=True) #echo=True for debugging
    engine = create_engine(db_uri)
    db = engine.connect()

    # carpark_ids = os.getenv('CARPARK_IDS').split(",")  # only send to specified car parks monthly customers

    # get the content for Octopus payment reminder SMS
    sql = text("""
        SELECT content from message where id = 5
    """)
    cursor = db.engine.execute(sql)
    msg = cursor.fetchone()
    octopus_content = msg['content']

    # get the content for Credit card payment reminder SMS
    sql = text("""
        SELECT content from message where id = 6
    """)
    cursor = db.engine.execute(sql)
    msg = cursor.fetchone()
    credit_card_content = msg['content']

    carpark_ids = get_supported_carparks()

    start_time = today.strftime("%Y-%m-%d 00:00:00")
    end_time = today.strftime("%Y-%m-%d 23:59:59")

    users = get_unpaid_users(carpark_ids, today)

    user_count = 0

    print("No of unpaid users: ", len(users))
    # print("Unpaid users: ", users)

    email_summary = ""

    for u in users:
        sql = text("""
            SELECT mobile.number, carpark.name as carpark_name, carpark.id as carpark_id, carpark.url_name, user.name, user.email, 
            mobile.user_id, user.preferred_payment_method, carpark_vehicle_type.monthly_rent_rate, carpark_config.accept_octopus_payment, carpark_config.accept_online_payment,
            user_carpark.special_rate  
            FROM mobile
            INNER JOIN user ON user.id = mobile.user_id AND user.date_deleted IS NULL
            INNER JOIN carpark ON carpark.id = :carpark_id
            INNER JOIN user_carpark ON user_carpark.user_id = user.id AND user_carpark.carpark_id = carpark.id
            INNER JOIN carpark_config ON carpark_config.carpark_id = carpark.id AND carpark_config.enable_monthly_rental = 1 
                AND (carpark_config.accept_online_payment = 1 OR carpark_config.accept_octopus_payment = 1)
            INNER JOIN vehicle ON user.id = vehicle.user_id AND vehicle.carpark_id = :carpark_id AND vehicle.is_default = 1
            INNER JOIN carpark_vehicle_type ON carpark_vehicle_type.vehicle_type_id = vehicle.vehicle_type_id AND carpark_vehicle_type.carpark_id = carpark.id
            WHERE mobile.is_verified = 1 AND mobile.is_default = 1
                AND mobile.date_deleted IS NULL AND user.id = :user_id
            ORDER BY carpark_name
        """).bindparams(user_id=u.user_id, carpark_id=u.carpark_id)
        contact = db.engine.execute(sql).fetchone()

        # print(contact)

        if contact is None:
            logging.error("No contact found for user ID: ", u.user_id)
            print("No contact found for user ID: ", u.user_id)
            continue

        # check if we have already sent sms to this user today
        sql = text("""
            SELECT id FROM user_message_log
            WHERE user_id = :user_id AND message_id IN (5,6) 
                AND date_created BETWEEN :start_time AND :end_time
        """).bindparams(user_id=u.user_id, start_time=start_time, end_time=end_time)
        is_sent_sms = db.engine.execute(sql).fetchone()

        if is_sent_sms:
            logging.info(f"SMS already sent to user ID: {u.user_id}. Skipping...")
            print(f"SMS already sent to user ID: {u.user_id}. Skipping...")
            continue

        country_code = "852"
        mobile_no = contact.number
        if os.getenv('DEV'):
            mobile_no = os.getenv('TEST_MOBILE_NO')

        if contact.accept_online_payment == 1 and contact.accept_octopus_payment == 1:
            if contact.preferred_payment_method == PAYMENT_METHOD_OCTOPUS:
                content = octopus_content
                msgid = 5
            else:
                content = credit_card_content
                msgid = 6
        elif contact.accept_online_payment == 1:
            content = credit_card_content
            msgid = 6
        elif contact.accept_octopus_payment == 1:
            # if only Octopus payment supported, use below custom message
            content = "「{carpark_name}」你的月租車位{days}會過期了。請盡快去停車場用已登記八達通卡繳付下個月{amount}的租金。多謝支持。"
            msgid = 5
        else:
            print(
                f"SMS not sent. Neither Octopus or Online payment is accepted by this carpark (id: {contact.carpark_id})")
            continue

        content = content.replace("{carpark_name}", f"{contact.carpark_name}車場")

        if remaining_days == 0:
            content = content.replace("{days}", "聽日")
        else:
            content = content.replace("{days}", "就快")
            #content = content.replace("{days}", f"{remaining_days}日後")

        content = content.replace("{amount}", f"${contact.special_rate or contact.monthly_rent_rate}")

        token_length = 8
        alphabet = string.ascii_letters + string.digits
        login_token = ''.join(secrets.choice(alphabet) for i in range(token_length))
        sql = text("""
            INSERT INTO user_login_token 
            (user_id, token, expires) VALUES (:user_id, :token, :expires)
        """).bindparams(user_id=contact.user_id, token=login_token,
                        expires=datetime.now().date() + relativedelta(days=remaining_days + 1))
        db.engine.execute(sql)

        content = content.replace("{payment_link}",
                                  f"{os.getenv('BASE_URL')}/portal/payment/?tk={login_token}")

        params = {
            "apiusername": os.getenv('ONE_WAY_SMS_API_USERNAME'),
            "apipassword": os.getenv('ONE_WAY_SMS_API_PASSWORD'),
            "senderid": "Uvision",
            "languagetype": 2,
            "mobileno": f"{country_code}{mobile_no}",
            "message": content
        }
        # logging.debug(f"Send SMS params: {params}")
        if os.getenv('DEV'):
            print(f"Mock SMS sent to uid {contact.user_id}:")
            logging.info(params)
        else:
            send_sms(params)

        sql = text("""
                    INSERT INTO user_message_log (user_id, message_id, content)
                    VALUES (:user_id, :message_id, :content)
                """).bindparams(user_id=u.user_id, message_id=msgid, content=content)

        msg_log = db.engine.execute(sql)

        email_summary += f"User ID: {contact.user_id} for {contact.carpark_name}車場 - carpark_id: {contact.carpark_id} - user_msg_log_id - {msg_log.lastrowid}<br/>Message: {content}<br/><br/><hr/><br/>"

        # logging.info("SMS reminder sent to user ID: ", contact.user_id, " carpark ID: ", contact.carpark_id)

        user_count += 1

        # if os.getenv('DEV') and user_count > 0:  # prevent test env from sending too many SMSes
        #     print("Exit on user_count =", user_count)
        #     break

    if user_count > 0:
        send_email('bugmagnet@gmail.com',
                   f"{user_count} SMS payment reminders sent on {datetime.today().strftime('%Y-%m-%d')}",
                   f"{user_count} SMS payment reminders sent to the following users:<br/><br/>{email_summary}")

    logging.info(f"{user_count} SMS reminders sent")


# Send email to our staff with list of unpaid customers
def email_unpaid_customer_summary():
    global db

    db_uri = os.getenv('DB_URI')

    # engine = create_engine(db_uri, echo=True) #echo=True for debugging
    engine = create_engine(db_uri)
    db = engine.connect()

    # carpark_ids = os.getenv('CARPARK_IDS').split(",")  # only send to specified car parks monthly customers
    carpark_ids = get_supported_carparks()

    last_month = today - relativedelta(months=1)
    # get last month's unpaid users for email summary because this is sent on 1st of the month
    users = get_unpaid_users(carpark_ids, last_month)
    user_count = 0
    user_stats = []

    print("No of unpaid users for summary email: ", len(users))

    email_content = "<style>td {padding: 5px;}</style>"
    email_content = "現在有{user_count}個月租客還沒付" + last_month.strftime("%-m") + "月的租金：<br/>"

    for u in users:
        sql = text("""
            SELECT mobile.number, carpark.name as carpark_name, carpark.url_name, user.name, user.email, 
            user.id as user_id, user.preferred_payment_method, carpark_vehicle_type.monthly_rent_rate, 
            payment_method.name_cn as payment_method_name_cn, vehicle.license, user_carpark.special_rate
            FROM mobile
            INNER JOIN user ON user.id = mobile.user_id AND user.date_deleted IS NULL
            INNER JOIN carpark ON carpark.id = :carpark_id
            INNER JOIN user_carpark ON user_carpark.user_id = user.id AND user_carpark.carpark_id = carpark.id
            LEFT JOIN payment_method ON payment_method.id = user.preferred_payment_method
            INNER JOIN vehicle ON user.id = vehicle.user_id AND vehicle.carpark_id = :carpark_id AND vehicle.is_default = 1
            INNER JOIN carpark_vehicle_type ON carpark_vehicle_type.vehicle_type_id = vehicle.vehicle_type_id AND carpark_vehicle_type.carpark_id = carpark.id
            WHERE mobile.is_verified = 1 AND mobile.is_default = 1
                AND mobile.date_deleted IS NULL AND user.id = :user_id
            ORDER BY carpark_name
        """).bindparams(user_id=u.user_id, carpark_id=u.carpark_id)
        contact = db.engine.execute(sql).fetchone()

        if contact is None:
            print("No contact found for user ID: ", u.user_id)
            continue

        user_stats.append({
            'rental_id': u.rental_id,
            'user_id': contact.user_id,
            'carpark_name': contact.carpark_name,
            'mobile': contact.number,
            'name': contact.name,
            'email': contact.email,
            'license': contact.license,
            'preferred_payment_method': contact.payment_method_name_cn or '--',
            'rate': contact.special_rate or contact.monthly_rent_rate
        })

        user_count += 1

    carpark_name = ''
    if user_count > 0:
        for item in user_stats:
            if item['carpark_name'] != carpark_name or carpark_name == '':
                carpark_name = item['carpark_name']
                if carpark_name != '':
                    email_content += "</table>"
                email_content += f"<h3>{carpark_name}車場沒付款的客：</h3>"
                email_content += '<table width="100%">'
                email_content += '<tr><th width="10%" style="border: 1px solid;">用戶ID</th><th width="10%" style="border: 1px solid;">月租ID</th><th width="10%" style="border: 1px solid;">姓名</th><th width="10%" style="border: 1px solid;">車牌</th><th width="20%" style="border: 1px solid;">手機</th>'
                email_content += '<th width="20%" style="border: 1px solid;">郵箱</th><th width="10%" style="border: 1px solid;">租金</th><th width="10%" style="border: 1px solid;">付款方式</th></tr>'

            email_content += f"<tr><td style=\"border: 1px solid;\">{item['user_id']}</td><td style=\"border: 1px solid;\">{item['rental_id']}</td><td style=\"border: 1px solid;\">{item['name'] or '--'}</td><td style=\"border: 1px solid;\">{item['license'] or '--'}</td><td style=\"border: 1px solid;\">{item['mobile']}</td><td style=\"border: 1px solid;\">{item['email'] or '--'}</td><td style=\"border: 1px solid;\">${item['rate']}.00</td><td style=\"border: 1px solid;\">{item['preferred_payment_method']}</td></tr>"

        email_content += "</table><br/><br/><br/><br/><br/>"

        email_content = email_content.replace("{user_count}", str(user_count))

        next_month = datetime.today() + relativedelta(months=1)
        send_email(os.getenv('STAFF_EMAIL'), f"沒有支付{next_month.strftime('%Y年%m月')}租金的月租客", email_content)

        print("Unpaid customer summary email sent to staff")


# Main program
load_dotenv()

today = datetime.today()
# today = datetime(2023, 8, 31)  # TODO: Comment out this line

last_day_of_month = calendar.monthrange(today.year, today.month)[1]
remaining_days = last_day_of_month - today.day
#remaining_days = 2  # TODO: Comment this out
# print("Remaining days:", remaining_days)

# only send payment reminder to users on the 25th, 28th and last day of the month
#if remaining_days == 5 or remaining_days == 2 or remaining_days == 0:
if remaining_days == 5 or remaining_days == 7:
    send_payment_reminder()

# only send unpaid customer list to staff on 1st day of month and 3 days before end of month
if today.day == 1 or remaining_days == 4:
    email_unpaid_customer_summary()
