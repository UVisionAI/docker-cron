import os, sys
from dotenv import load_dotenv
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

db_uri = os.getenv('DB_URI')
engine = create_engine(db_uri)
db = engine.connect()

Session = sessionmaker(bind=engine)
session = Session()

try:
    # Delete all expired user_login_tokens
    sql = text("""
        DELETE FROM user_login_token WHERE expires < DATE_SUB(NOW(), INTERVAL 7 DAY) 
    """)
    result = session.execute(sql)

    session.commit()

    print("Expired user_login_tokens deleted")

except Exception as e:
    print(e)
    session.rollback()
    print(e)

# First get all customers who have been flagged for deletion over a week ago
sql = text("""
    SELECT user.id FROM user WHERE user.date_deleted < DATE_SUB(NOW(), INTERVAL 14 DAY)
    """)
cursor = db.engine.execute(sql)
users = cursor.fetchall()

if not users:
    print("No users flagged for deletion")
    raise SystemExit(0)

print(f"{len(users)} flagged for deletion")
for user in users:
    try:
        print(f"Deleting user_id: {user.id}")
        sql = text("""
                    DELETE from octopus where user_id = :user_id
                """).bindparams(user_id=user.id)
        result = session.execute(sql)

        sql = text("""
                        DELETE from user_carpark where user_id = :user_id
                    """).bindparams(user_id=user.id)
        result = session.execute(sql)

        sql = text("""
                DELETE from user_carpark_rental where user_id = :user_id
            """).bindparams(user_id=user.id)
        result = session.execute(sql)

        sql = text("""
                UPDATE user_message_log SET user_id=NULL where user_id = :user_id
            """).bindparams(user_id=user.id)
        result = session.execute(sql)

        sql = text("""
                DELETE from vehicle where user_id = :user_id
            """).bindparams(user_id=user.id)
        result = session.execute(sql)

        sql = text("""
                    DELETE from mobile where user_id = :user_id
                """).bindparams(user_id=user.id)
        result = session.execute(sql)

        sql = text("""
                DELETE FROM user_login_token WHERE user_id = :user_id 
            """).bindparams(user_id=user.id)
        result = session.execute(sql)

        sql = text("""
                DELETE FROM user_login_token WHERE user_id = :user_id 
            """).bindparams(user_id=user.id)
        result = session.execute(sql)

        # Delete all customer details from related tables
        sql = text("""
            DELETE from user where id = :user_id
        """).bindparams(user_id=user.id)
        result = session.execute(sql)

        session.commit()

        print(f"Deleted user: {user.id}")

    except Exception as e:
        print(e)
        session.rollback()
        continue
