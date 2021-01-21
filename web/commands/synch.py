
import logging
from collections import deque

from airtable import Airtable

SHORT_STRING_MAX_LENGTH = 144
URL_MAX_LENGTH = 512

CREATE_COMPANIES_TABLE_QUERY = f"""
create table if not exists companies (
  id integer primary key autoincrement,
  airtable_key char(17) not null,
  name varchar({SHORT_STRING_MAX_LENGTH}),
  location varchar({SHORT_STRING_MAX_LENGTH}),
  web_url varchar({URL_MAX_LENGTH}),
  linkedin_url varchar({URL_MAX_LENGTH})
)
"""

CREATE_JOBS_TABLE_QUERY = f"""
create table if not exists jobs (
  id integer primary key autoincrement,
  airtable_key char(17) not null,
  title varchar({SHORT_STRING_MAX_LENGTH}) not null,
  description text not null,
  location varchar({SHORT_STRING_MAX_LENGTH}),
  requirements text,
  responsibilities text,
  salary_range text,
  hiring_process text,
  company_id int,
  foreign key(company_id) REFERENCES companies(id)
);
"""

CREATE_TAGS_TABLE_QUERY = """
create table if not exists tags (
  id integer primary key autoincrement,
  airtable_key char(17) not null,
  name varchar(144)
);
"""

CREATE_JOB_TAGS_TABLE_QUERY = """
create table if not exists job_tags (
  id integer primary key autoincrement,
  job_id int,
  tag_id int,
  foreign key(job_id) references jobs(id),
  foreign key(tag_id) references tags(id)
)
"""

CREATE_COMPANIES_AIRTABLE_KEY_INDEX_QUERY = """
create index if not exists "company_airtable_key_index" on "companies" (
  "airtable_key" ASC
);
"""

CREATE_JOBS_AIRTABLE_KEY_INDEX_QUERY = """
create index if not exists"jobs_airtable_key_index" on "jobs" (
  "airtable_key" ASC
);
"""

CREATE_TAGS_AIRTABLE_KEY_INDEX_QUERY = """
create index if not exists"tags_airtable_key_index" on "tags" (
  "airtable_key" ASC
);
"""

# Returns list of values from dictionary as flat list.
dictvals = lambda d, ks: [d.get(k) for k in ks]

# Flattens list. ([(1, 2), (3, 4)] -> [1, 2, 3, 4]
flatten = lambda l: [item for sublist in l for item in sublist]


def create_tables(db, cr):
    cr.execute(CREATE_COMPANIES_TABLE_QUERY)
    logging.info('company table created')
    cr.execute(CREATE_JOBS_TABLE_QUERY)
    logging.info('job table created ')
    cr.execute(CREATE_TAGS_TABLE_QUERY)
    logging.info('tag table created')
    cr.execute(CREATE_JOB_TAGS_TABLE_QUERY)
    logging.info('job tag table created')
    cr.execute(CREATE_COMPANIES_AIRTABLE_KEY_INDEX_QUERY)
    logging.info('Company air table key index created')
    cr.execute(CREATE_JOBS_AIRTABLE_KEY_INDEX_QUERY)
    logging.info('Company air table key index created')
    cr.execute(CREATE_TAGS_AIRTABLE_KEY_INDEX_QUERY)
    logging.info('Company air table key index created')
    db.commit()


def get_list_from_table(table_name, field_name, db, cr):
    """ Get field name from table and return as flat list.
    """
    cr.execute(f'SELECT {field_name} FROM {table_name}')
    return [r[0] for r in cr.fetchall()]


def delete_by_field(table_name, key, value, db, cr):
    """ Delete record from table where key = value.
    """
    cr.execute(f'DELETE FROM {table_name} WHERE {key} = ?',
               [value, ])
    db.commit()

def db_id_of_airtable_key(table_name, airtable_key, cr):
    """ Get id of database record from it's airtable_key.
    """
    cr.execute(f'SELECT id FROM {table_name} WHERE airtable_key=?',
               [airtable_key, ])
    try:
        return cr.fetchone()[0]
    except TypeError:
        return None


def get_company_values(company_record):
    """ Extract values from company_record of airtable to put on database.

    Returns:
    ['airtable_key', 'name', 'location', 'web_url', 'linkedin_url']
    """
    values = [company_record['id'], ]
    values.extend(dictvals(company_record['fields'],
                           ['Name', 'Location', 'Web Url', 'Linkedin Url']))
    return values


def get_tag_values(tag_record):
    """ Extract values from tag_record of airtable to put on database.

    Returns:
    ['airtable_key', 'name']
    """

    values = [tag_record['id'], ]
    values.extend(dictvals(tag_record['fields'], ['Name', ]))
    return values


def get_job_values(job_record, cr):
    """ Extract values from job_record of airtable to put on database.

    Returns:
    ['airtable_key', 'title', 'description', 'location', 'requirements',
     'responsibilities', 'salary_range', 'hiring_process', 'company_id']
    """
    values = [job_record['id'], ]
    values.extend(
        dictvals(job_record['fields'], ['Title', 'Description', 'Location',
                                        'Requirements', 'Responsibilities',
                                        'Salary Range', 'Hiring Process,']))
    try:
        values.append(
            db_id_of_airtable_key(
                'companies', job_record['fields']['Company'][0], cr))
    except KeyError:  # There's no company.
        values.append(None)
    return values


def get_job_tag_values(job_record, airtable_key_of_tag, cr):
    """
    Extract values from job_record of airtable to put on database.

    Returns:
    ['job_id', 'tag_id']
    """
    return (db_id_of_airtable_key('jobs', job_record['id'], cr),
            db_id_of_airtable_key('tags', airtable_key_of_tag, cr))


def create_company(company_record, db, cr):
    cr.execute('''
    INSERT INTO companies (
        airtable_key, name, location, web_url, linkedin_url
    ) VALUES(?, ?, ?, ?, ?);
    ''', get_company_values(company_record))
    db.commit()


def update_company(company_record, db, cr):
    values = deque(get_company_values(company_record))
    values.rotate(-1)  # Put airtable_key at 0 to end of the list.
    cr.execute('''
    UPDATE companies SET name = ?, location = ?, web_url = ?, linkedin_url = ?
    WHERE airtable_key = ?;
    ''', values)
    db.commit()


def create_tag(tag_record, db, cr):
    cr.execute('''
    INSERT INTO tags (airtable_key, name) VALUES(?, ?);
    ''', get_tag_values(tag_record))
    db.commit()


def update_tag(tag_record, db, cr):
    values = deque(get_tag_values(tag_record))
    values.rotate(-1)  # Put airtable_key to end of the list.
    cr.execute('''
    UPDATE tags SET name = ? WHERE airtable_key = ?;
    ''', values)
    db.commit()


def create_job(job_record, db, cr):
    values = get_job_values(job_record, cr)
    cr.execute('''
    INSERT INTO jobs (
        airtable_key, title, description, location, requirements,
        responsibilities, salary_range, hiring_process, company_id
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?
    );
    ''', values)
    db.commit()


def update_job(job_record, db, cr):
    values = deque(get_job_values(job_record, cr))
    values.rotate(-1)  # Put airtable_key to end of the list.
    cr.execute('''
    UPDATE jobs SET title = ?, description = ?, location = ?,
                    requirements = ?, responsibilities = ?, salary_range = ?,
                    hiring_process = ?, company_id = ?
    WHERE airtable_key = ?;
    ''', list(values))


def create_job_tag(job_tag_values, db, cr):
    cr.execute('''
    INSERT INTO job_tags (job_id, tag_id) VALUES (?, ?);
    ''', job_tag_values)
    db.commit()
    return cr.lastrowid


def set_company_records_on_db(airtable_base_key, airtable_api_key, db, cr):
    airtable = Airtable(airtable_base_key, 'Companies', airtable_api_key)
    airtable_keys_on_table = get_list_from_table('companies',
                                                 'airtable_key', db, cr)
    company_records_on_airtable = airtable.get_all()
    for company_record in company_records_on_airtable:
        if company_record['id'] in airtable_keys_on_table:
            update_company(company_record, db, cr)
            logging.info('updated company: ' + company_record['id'])
        else:
            create_company(company_record, db, cr)
            logging.info('created company: ' + company_record['id'])

    airtable_keys_on_airtable = [company_record['id'] for company_record in
                                 company_records_on_airtable]
    airtable_keys_to_delete = \
        set(airtable_keys_on_table) - set(airtable_keys_on_airtable)
    for airtable_key in airtable_keys_to_delete:
        delete_by_field('companies', 'airtable_key', airtable_key)
        logging.info('deleted company: ' + company_record['id'])


def set_tag_records_on_db(airtable_base_key, airtable_api_key, db, cr):
    airtable = Airtable(airtable_base_key, 'Tags', airtable_api_key)
    airtable_keys_on_table = get_list_from_table(
        'tags', 'airtable_key', db, cr)
    tag_records_on_airtable = airtable.get_all()
    for tag_record in tag_records_on_airtable:
        if tag_record['id'] in airtable_keys_on_table:
            update_tag(tag_record, db, cr)
            logging.info('updated tag: ' + tag_record['id'])
        else:
            create_tag(tag_record, db, cr)
            logging.info('created tag: ' + tag_record['id'])
    airtable_keys_on_airtable = [tag_record['id'] for tag_record in
                                 tag_records_on_airtable]
    airtable_keys_to_delete = \
        set(airtable_keys_on_table) - set(airtable_keys_on_airtable)
    for airtable_key in airtable_keys_to_delete:
        delete_by_field('tags', 'airtable_key', airtable_key, db, cr)
        logging.info('deleted tag: ' + tag_record['id'])


def set_job_tag_records_on_db(job_tags_values_list, db, cr):
    query = 'SELECT id, job_id, tag_id from job_tags where '
    for idx in range(len(job_tags_values_list)):
        query += '(job_id = ? and tag_id = ?) or '
    query = query[:-3]
    cr.execute(query, flatten(job_tags_values_list))
    records_on_db = cr.fetchall()
    job_tag_ids_on_db = [r[0] for r in records_on_db]
    job_tag_values_on_table = [r[1:] for r in records_on_db]
    for job_tag_values in job_tags_values_list:
        if job_tag_values in job_tag_values_on_table:
            continue
        job_tag_id = create_job_tag(job_tag_values, db, cr)
        job_tag_ids_on_db.append(job_tag_id)
        logging.info(f'created job tag: {job_tag_id}')
    db.commit()

    # Delete unused job tag rows.
    delete_unused_rows_query = 'DELETE FROM job_tags WHERE id not in ('
    delete_unused_rows_query += '?, ' * len(job_tag_ids_on_db)
    delete_unused_rows_query = delete_unused_rows_query[:-2] + ')'
    cr.execute(delete_unused_rows_query, job_tag_ids_on_db)
    db.commit()
    if cr.rowcount > 0:
        logging.info(f'Num of deleted job tags from DB {cr.rowcount}')


def set_job_records_on_db(airtable_base_key, airtable_api_key, db, cr):
    airtable = Airtable(airtable_base_key, 'Jobs', airtable_api_key)
    airtable_keys_on_table = get_list_from_table(
        'jobs', 'airtable_key', db, cr)
    job_records_on_airtable = airtable.get_all()
    job_tags_values = []
    for job_record in job_records_on_airtable:
        if 'Title' not in job_record['fields']:
            continue
        if job_record['id'] in airtable_keys_on_table:
            update_job(job_record, db, cr)
            logging.info('updated job: ' + job_record['id'])
        else:
            create_job(job_record, db, cr)
            logging.info('created job: ' + job_record['id'])

        for airtable_key_of_tag in job_record['fields'].get('Tags', []):
            job_tags_values.append(get_job_tag_values(
                job_record, airtable_key_of_tag, cr))
    airtable_keys_on_airtable = [job_record['id'] for job_record in
                                 job_records_on_airtable]
    airtable_keys_to_delete = \
        set(airtable_keys_on_table) - set(airtable_keys_on_airtable)
    for airtable_key in airtable_keys_to_delete:
        delete_by_field('jobs', 'airtable_key', airtable_key, db, cr)
        logging.info('deleted job: ' + job_record['id'])
    return job_tags_values


def synch_db_from_airtable(airtable_base_key, airtable_api_key, db, cr):
    create_tables(db, cr)
    set_company_records_on_db(airtable_base_key, airtable_api_key, db, cr)
    set_tag_records_on_db(airtable_base_key, airtable_api_key, db, cr)
    job_tags_values_list = set_job_records_on_db(
        airtable_base_key, airtable_api_key, db, cr
    )
    set_job_tag_records_on_db(job_tags_values_list, db, cr)
