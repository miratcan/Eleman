from os import environ

DEFAULT_DATABASE = environ.get('DEFAULT_DATABASE', 'db.sqlite3')
JOBS_PER_PAGE = int(environ.get('JOBS_PER_PAGE', 20))
SITE_TITLE = environ.get('SITE_TITLE', 'Site Title')
SITE_DESC = environ.get('SITE_DESC', 'Site Description')

AIRTABLE_BASE_KEY = environ.get('AIRTABLE_BASE_KEY')
AIRTABLE_API_KEY = environ.get('AIRTABLE_API_KEY')
