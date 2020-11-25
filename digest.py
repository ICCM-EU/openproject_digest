import psycopg2
import psycopg2.extras
import sqlite3
import os.path
import re
import datetime
import jinja2
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

CONTENT_TYPE_MESSAGES = 1
CONTENT_TYPE_TASKS = 2
LENGTH_EXCERPT = 100
DEBUG = False

configfile="/etc/openproject/conf.d/00_addon_postgres"
if os.path.isfile(configfile):
  f = open(configfile, "r")
  # export DATABASE_URL="postgres://openproject:topsecret@127.0.0.1:45432/openproject"
  config=f.read()
  dbuser=re.search(r"\/\/(.*?):", config).group(1)
  dbpwd=re.search(r":([^:]*)@", config).group(1)
  dbport=re.search(r":([0-9]+)\/", config).group(1)
  dbname=re.search(r"\/([^/]*?)\"", config).group(1)
  dbhost=re.search(r"@(.*):", config).group(1)

configfile="/etc/openproject/conf.d/server"
if os.path.isfile(configfile):
  f = open(configfile, "r")
  # export SERVER_HOSTNAME="dev.openproject.pokorra.de"
  # export SERVER_PROTOCOL="http"
  pageurl = ""
  for line in f:
    if "SERVER_HOSTNAME" in line:
      hostname = re.search(r"\"(.*?)\"", line).group(1)
      pageurl += hostname
    if "SERVER_PROTOCOL" in line:
      pageurl = re.search(r"\"(.*?)\"", line).group(1) + "://" + pageurl

configfile="/etc/openproject/conf.d/smtp"
if os.path.isfile(configfile):
  f = open(configfile, "r")
  # export EMAIL_DELIVERY_METHOD="smtp"
  for line in f:
    if "ADMIN_EMAIL" in line:
      admin_email = re.search(r"\"(.*?)\"", line).group(1)
    if "SMTP_HOST" in line:
      smtp_host = re.search(r"\"(.*?)\"", line).group(1)
    if "SMTP_PORT" in line:
      smtp_port = re.search(r"\"(.*?)\"", line).group(1)
    if "SMTP_USERNAME" in line:
      smtp_username = re.search(r"\"(.*?)\"", line).group(1)
    if "SMTP_PASSWORD" in line:
      smtp_password = re.search(r"\"(.*?)\"", line).group(1)

# Connect to your postgres DB
params = {'dbname': dbname, 'user': dbuser, 'password': dbpwd, 'port': dbport, 'host': dbhost}
conn = psycopg2.connect(**params)
cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)

sq3 = sqlite3.connect('notifications.sqlite3')
sq3.execute("""
CREATE TABLE IF NOT EXISTS Notified (
id INTEGER PRIMARY KEY AUTOINCREMENT,
user_id INTEGER,
project_id INTEGER,
content_type INTEGER,
content_id INTEGER,
t TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")


# get all users per project
sqlUsersProjects = """
select users.id, mail, login, firstname, lastname, value as frequency, '' as account_url, project_id, projects.name as project_name
from users, custom_values, custom_fields, members, projects
where customized_id = users.id and custom_field_id = custom_fields.id 
and custom_fields.name='FrequencyNotification' and users.type='User' 
and users.status=1 and users.id = members.user_id
and members.project_id = projects.id"""

# get all posts per project within the past 2 weeks
sqlForumMessages = """
select messages.id, forum_id, parent_id, subject, firstname, lastname, login, messages.created_on, content, '' as url
from messages, users, forums
where messages.author_id = users.id and forums.id = messages.forum_id and forums.project_id = %s order by created_on asc"""

# verify if user has already been notified abou this content
sqlCheckNotification = """
select *
from Notified
where user_id = ? and project_id = ? and content_id = ? and content_type = ?"""

sqlAddNotification = """
insert into Notified(user_id, project_id, content_id, content_type)
values(?,?,?,?)"""

def processUser(frequency):
  now = datetime.datetime.now()

  # frequency: every 3 hours, send at 7 am, 10 am, 1 pm, 4 pm, 7 pm, 10 pm, 1 am, 4 am
  if frequency == 1:
    if now.hour not in [7,10,13,16,19,22,1,4]:
      return False

  # frequency: daily, send at 7 am
  if frequency == 2:
    if now.hour != 7:
      return False

  # frequency: weekly, send at 7 pm on Saturday
  if frequency == 3:
    if now.hour != 19 and now.weekday != 5:
      return False

  # frequency: never
  if frequency == 4:
    return False

  return True

def alreadyNotified(userId, projectId, contentId, contentType):
  cursor = sq3.cursor()
  cursor.execute(sqlCheckNotification, (userId, projectId, contentId, contentType,))
  row = cursor.fetchone()
  if row is None:
    return False
  return True

def storeNotified(userId, projectId, contentId, contentType):
  sq3.execute(sqlAddNotification, (userId, projectId, contentId, contentType,))

def storeAllNotified(user, messages):
  for post in messages:
    storeNotified(user['id'], user['project_id'], post['id'], CONTENT_TYPE_MESSAGES)
  sq3.commit()

def sendMail(user, messages):
  if len(messages) == 0:
    return

  file_loader = jinja2.FileSystemLoader('templates')
  env = jinja2.Environment(loader=file_loader)
  template = env.get_template('digest.html')
  output = template.render(user=user, messages=messages)

  try:
    context = ssl.create_default_context()
    server = smtplib.SMTP(smtp_host, smtp_port)
    server.starttls(context=context) 
    server.login(smtp_username, smtp_password)
    msg = MIMEMultipart('alternative')
    msg['Subject'] = ("%s OpenProject Digest" % (user['project_name'],))
    msg['From'] = "no_reply@" + hostname
    msg['To'] = user['mail']
    msg.attach(MIMEText(output, 'html'))

    if DEBUG:
      print(msg.as_string())
    else:
      server.sendmail(msg['From'], msg['To'], msg.as_string())
  except Exception as e:
    print(e)
  finally:
    server.quit()

  if DEBUG:
    # don't store in sqlite database
    return False

  return True


cur.execute(sqlUsersProjects)
rows = cur.fetchall()
for userRow in rows:
  # should we process this user now?
  if DEBUG or processUser(int(userRow['frequency'])):

    userRow['account_url'] = ("%s/my/account" % (pageurl,))

    # get all new messages
    messages = []
    cur.execute(sqlForumMessages, (userRow['project_id'],))
    posts = cur.fetchall()
    for p in posts:
      if not alreadyNotified(userRow['id'], userRow['project_id'], p['id'], CONTENT_TYPE_MESSAGES):
        if p['parent_id'] is None:
          p['parent_id'] = p['id']
        if len(p['content']) > LENGTH_EXCERPT:
          p['content'] = p['content'][0:LENGTH_EXCERPT] + "..."
        p['url'] = ("%s/topics/%s?r=%s#message-%s" % (pageurl, p['parent_id'], p['id'], p['id']))
        messages.append(p)

    if sendMail(userRow, messages):
      storeAllNotified(userRow, messages)



