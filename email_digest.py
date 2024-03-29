import psycopg2
import psycopg2.extras
import sqlite3
import os.path
from pathlib import Path
import re
import datetime
import jinja2
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pytz import timezone
import pytz

CONTENT_TYPE_MESSAGES = 1
CONTENT_TYPE_TASKS = 2
CONTENT_TYPE_MEETING_AGENDA = 3
CONTENT_TYPE_MEETING_MINUTES = 4
LENGTH_EXCERPT = 100
CUSTOMFIELD_FREQUENCY = "FrequencyDigest"
START_DATE = '2021-12-23'
DEBUG = False
localtz = timezone('Europe/Amsterdam')

dbname = None
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
else:
  configfile=("%s/openproject/config/database.yml" % (Path.home(),))
  if os.path.isfile(configfile):
    f = open(configfile, "r")
    for line in f:
      dbport = 5432
      # host: localhost
      if "host: " in line:
        dbhost = re.search(r": (.*)", line).group(1)
      if "database: " in line:
        dbname = re.search(r": (.*)", line).group(1)
      if "username: " in line:
        dbuser = re.search(r": (.*)", line).group(1)
      if "password: " in line:
        dbpwd = re.search(r": (.*)", line).group(1).replace('"', '')

if not dbname:
  print("could not find the config file")
  exit(-1)

# Connect to your postgres DB
params = {'dbname': dbname, 'user': dbuser, 'password': dbpwd, 'port': dbport, 'host': dbhost}
conn = psycopg2.connect(**params)
cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)

# Connect to the sqlite database
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
and custom_fields.name=%s and users.type='User' 
and users.status=1 and users.id = members.user_id
and members.project_id = projects.id"""

# get all posts per project within the past 2 weeks
sqlForumMessages = """
select messages.id, forum_id, parent_id, forums.name as forum_name, subject, firstname, lastname, login, messages.created_at, content, '' as url
from messages, users, forums
where messages.author_id = users.id and forums.id = messages.forum_id and forums.project_id = %s
and messages.created_at >= %s
order by created_at asc"""

# get all created or updated tasks per project within the past 2 weeks
sqlUpdatedTasks = """
select w.id, p.identifier as projectslug, w.subject, w.description, w.updated_at, '' as url
from work_packages as w, projects as p
where w.project_id = p.id
and w.project_id = %s
and w.updated_at >= %s
order by updated_at asc"""

# get all created or updated meetings per project within the past 2 weeks
sqlUpdatedMeetings = """
select mc.id, m.id as meeting_id, p.identifier as projectslug, m.title as subject, m.start_time, mc.type, mc.text as description, mc.updated_at, '' as url
from meetings as m, meeting_contents as mc, projects as p
where m.project_id = p.id
and m.project_id = %s
and mc.meeting_id = m.id
and mc.updated_at >= %s
order by mc.updated_at asc"""

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

def storeAllNotified(user, messages, tasks, meetings):
  for post in messages:
    storeNotified(user['id'], user['project_id'], post['id'], CONTENT_TYPE_MESSAGES)
  for task in tasks:
    storeNotified(user['id'], user['project_id'], task['id'], CONTENT_TYPE_TASKS)
  for meeting in meetings:
    if meeting['type'] == 'MeetingMinutes':
      storeNotified(user['id'], user['project_id'], meeting['id'], CONTENT_TYPE_MEETING_MINUTES)
    if meeting['type'] == 'MeetingAgenda':
      storeNotified(user['id'], user['project_id'], meeting['id'], CONTENT_TYPE_MEETING_AGENDA)
  sq3.commit()

def sendMail(user, messages, tasks, meetings):
  if (len(messages) == 0) and (len(tasks) == 0) and (len(meetings) == 0):
    return

  file_loader = jinja2.FileSystemLoader('templates')
  env = jinja2.Environment(loader=file_loader)
  template = env.get_template('digest.html')
  output = template.render(user=user, messages=messages, tasks=tasks, meetings=meetings)

  server = None
  try:
    msg = MIMEMultipart('alternative')
    msg['Subject'] = ("%s OpenProject Digest" % (user['project_name'],))
    msg['From'] = settings['admin_email']
    msg['To'] = user['mail']
    msg.attach(MIMEText(output, 'html'))

    if DEBUG:
      print(msg.as_string())
      print(output)
    else:
      print("sending email to %s" % (msg['To']))
      context = ssl.create_default_context()
      server = smtplib.SMTP(settings['smtp_host'], settings['smtp_port'])
      if settings['smtp_host'] != "localhost" and settings['smtp_enable_starttls_auto'] != '0':
        server.starttls(context=context)
      if settings['smtp_username']:
        server.login(settings['smtp_username'], settings['smtp_password'])
      result = server.sendmail(msg['From'], msg['To'], msg.as_string())
      if result:
        print("sending the email did not work")
        for element in result:
          print(element)
        return False
  except Exception as e:
    print(e)
  finally:
    if server is not None:
      server.quit()

  if DEBUG:
    # don't store in sqlite database
    return False

  return True

# get the settings for SMTP and the URL
def getSettings():
  sqlSettings = """
    select name, value
    from settings
    where name in ('mail_from', 'protocol', 'host_name', 'smtp_address', 'smtp_port', 'smtp_domain', 'smtp_user_name', 'smtp_password', 'smtp_enable_starttls_auto')"""
  cur.execute(sqlSettings)
  rows = cur.fetchall()
  settings = {}
  settings['pageurl'] = ''
  for row in rows:
    if row['name'] == 'host_name':
       settings['pageurl'] += row['value']
    if row['name'] == 'protocol':
       settings['pageurl'] = row['value'] + "://" + settings['pageurl']
    if row['name'] == 'mail_from':
       settings['admin_email'] = row['value']
    if row['name'] == 'smtp_address':
       settings['smtp_host'] = row['value']
    if row['name'] == 'smtp_port':
       settings['smtp_port'] = row['value']
    if row['name'] == 'smtp_user_name':
       settings['smtp_username'] = row['value']
    if row['name'] == 'smtp_password':
       settings['smtp_password'] = row['value']
    if row['name'] == 'smtp_domain':
       settings['smtp_domain'] = row['value']
    if row['name'] == 'smtp_enable_starttls_auto':
       settings['smtp_enable_starttls_auto'] = row['value']
  return settings

settings = getSettings()
cur.execute(sqlUsersProjects, (CUSTOMFIELD_FREQUENCY,))
rows = cur.fetchall()
if not rows:
  # fail if no CustomField exists
  print('We cannot find any users with custom field %s' % (CUSTOMFIELD_FREQUENCY,))
  exit(-1)
for userRow in rows:
  # should we process this user now?
  if DEBUG or processUser(int(userRow['frequency'])):

    userRow['account_url'] = ("%s/my/account" % (settings['pageurl'],))

    # get all new messages
    messages = []
    cur.execute(sqlForumMessages, (userRow['project_id'],START_DATE,))
    posts = cur.fetchall()
    for p in posts:
      if not alreadyNotified(userRow['id'], userRow['project_id'], p['id'], CONTENT_TYPE_MESSAGES):
        if p['parent_id'] is None:
          p['parent_id'] = p['id']
        if len(p['content']) > LENGTH_EXCERPT:
          p['content'] = p['content'][0:LENGTH_EXCERPT].strip() + "[...]"
        #p['created_at'] = pytz.utc.localize(p['created_at'], is_dst=False).astimezone(localtz)
        p['url'] = ("%s/topics/%s?r=%s#message-%s" % (settings['pageurl'], p['parent_id'], p['id'], p['id']))
        messages.append(p)

    # get all new or updated tasks
    tasks = []
    cur.execute(sqlUpdatedTasks, (userRow['project_id'],START_DATE,))
    items = cur.fetchall()
    for p in items:
      if not alreadyNotified(userRow['id'], userRow['project_id'], p['id'], CONTENT_TYPE_TASKS):
        if p['description'] is not None and len(p['description']) > LENGTH_EXCERPT:
          p['description'] = p['description'][0:LENGTH_EXCERPT].strip() + "[...]"
        p['url'] = ("%s/projects/%s/work_packages/%s/activity" % (settings['pageurl'], p['projectslug'], p['id']))
        tasks.append(p)

    # get all new or updated meetings
    meetings = []
    cur.execute(sqlUpdatedMeetings, (userRow['project_id'],START_DATE,))
    items = cur.fetchall()
    for p in items:
      if not p['description']:
        # do not send out notifications for empty items
        continue
      if p['type'] == 'MeetingAgenda':
        if not alreadyNotified(userRow['id'], userRow['project_id'], p['id'], CONTENT_TYPE_MEETING_AGENDA):
          if len(p['description']) > LENGTH_EXCERPT:
            p['description'] = p['description'][0:LENGTH_EXCERPT].strip() + " [...]"
          p['url'] = ("%s/meetings/%s/agenda" % (settings['pageurl'], p['meeting_id']))
          meetings.append(p)
      if p['type'] == 'MeetingMinutes':
        if not alreadyNotified(userRow['id'], userRow['project_id'], p['id'], CONTENT_TYPE_MEETING_MINUTES):
          if len(p['description']) > LENGTH_EXCERPT:
            p['description'] = p['description'][0:LENGTH_EXCERPT].strip() + " [...]"
          p['url'] = ("%s/meetings/%s/minutes" % (settings['pageurl'], p['meeting_id']))
          meetings.append(p)

    if sendMail(userRow, messages, tasks, meetings):
      storeAllNotified(userRow, messages, tasks, meetings)



