Digest for OpenProject
======================

OpenProject is an Open source project management software. See https://www.openproject.org/

This is a Python script, that queries the PostgreSQL database for the latest updates to the forum, tasks, meetings, etc. and regularly sends E-Mails to the users.

You must add a custom setting to your OpenProject at https://your.openproject.example.org/custom_fields?tab=UserCustomField. It should be called FrequencyNotification, it should be a list and have these values in this order: every 3 hours, daily (default), weekly, disabled

The CustomField FrequencyNotification should be required, visible and editable.

To run this script, setup a virtual environment, and install the required packages:

    virtualenv -p /usr/bin/python3 .venv
    source .venv/bin/activate
    pip install -r requirements.txt



