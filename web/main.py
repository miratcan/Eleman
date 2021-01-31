import logging
import sqlite3
import subprocess
from os import environ
from os.path import dirname, join
from urllib.parse import urlencode


import flask
from flask import render_template, request
from flask_cors import CORS
from flask_misaka import Misaka

from commands.synch import synch_db_from_airtable


app = flask.Flask(__name__)
app.config.from_pyfile('settings.py')

CORS(app)
Misaka(app)

db = sqlite3.connect(app.config['DEFAULT_DATABASE'], check_same_thread=False)
cr = db.cursor()


@app.cli.command("compile-css")
def compile_css():
    """
    Command for compiling CSS. You have to run that if you made changes on
    web.css file.
    """
    input_path = join(dirname(__file__), "assets", "web.css")
    output_path = join(dirname(__file__), "static", "web.css")
    compile_css_files_command = subprocess.run([
        "tailwindcss", "build", input_path , "-o", output_path
    ])


@app.cli.command("synch")
def synch():
    AIRTABLE_BASE_ID = app.config['AIRTABLE_BASE_ID']
    AIRTABLE_API_KEY = app.config['AIRTABLE_API_KEY']
    if AIRTABLE_BASE_ID is None or AIRTABLE_API_KEY is None:
        raise Exception("AIRTABLE_API_KEY and AIRTABLE_BASE_ID environment "
                        "variables must be set to run this command.")
    synch_db_from_airtable(AIRTABLE_BASE_ID, AIRTABLE_API_KEY, db, cr)


@app.context_processor
def inject_site_info():
    return {
        "SITE_TITLE": app.config['SITE_TITLE'],
        "SITE_DESC": app.config['SITE_DESC'],
    }


JOB_TABLE_KEYS = "title", "description", "location", "requirements", \
                 "responsibilities", "salary_range", "hiring_process", \
                 "company_name"

app.config['TEMPLATES_AUTO_RELOAD'] = True


def build_jobs_query(text_to_search=None, tag_name=None, page=0):
    params = []
    query = """
    select
        jobs.id, title, description, jobs.location, requirements,
        responsibilities, salary_range, hiring_process,
        companies.name as company_name
    from jobs
        inner join companies on jobs.company_id=companies.id
    """
    if text_to_search or tag_name:
        query += "where "
        if text_to_search:
            query += "title like ? "
            params.append(f'%{text_to_search}%')
            if tag_name:
                query += "and "
        if tag_name:
            query += "jobs.id in (" \
                     "select job_tags.job_id from job_tags join tags " \
                     "on tags.id=job_tags.tag_id where tags.name=?) "
            params.append(tag_name)

    query += f"limit ?, {app.config['JOBS_PER_PAGE']}"
    params.append(page)
    return query, params


def build_job_count_query(text_to_search=None, tag_name=None):
    params = []
    query = """
    select count(jobs.id)
    from jobs
    inner join companies on jobs.company_id=companies.id
    """
    if text_to_search or tag_name:
        query += "where "
        if text_to_search:
            query += "title like ? "
            params.append(f'%{text_to_search}%')
            if tag_name:
                query += "and "
        if tag_name:
            query += "jobs.id in (" \
                     "select job_tags.job_id from job_tags join tags " \
                     "on tags.id=job_tags.tag_id where tags.name=?) "
            params.append(tag_name)
    return query, params


def build_tags_query(job_ids):
    question_marks = "?".join([','] * (len(job_ids) + 1))[1:-1]
    query = f"""
    select
        job_id, tags.name
    from job_tags
    inner join tags on tags.id=job_tags.tag_id
    where job_tags.job_id in ({question_marks})
    """
    return query


def get_job_listing_context(text_to_search=None, tag_name=None, page=0):
    """
        Returns paginated list of jobs.
    """

    count_query, count_params = build_job_count_query(text_to_search, tag_name)
    cr.execute(count_query, count_params)
    jobs_count = cr.fetchone()[0]

    page_range = []
    jobs_per_page = app.config['JOBS_PER_PAGE']
    for page_num in list(range(1, int(jobs_count / jobs_per_page) + 1)):
        params = {'p': page_num, 'q': text_to_search, 't' : tag_name}
        params = dict(filter(lambda i: i[1] is not None, params.items()))
        page_range.append({
            'page_num': page_num,
            'url': f'/?{urlencode(params)}',
            'is_current': page == page_num - 1
        })

    query, params = build_jobs_query(text_to_search, tag_name, page)

    cr.execute(query, params)
    rows = cr.fetchall()

    jobs = {}

    for row in rows:
        jobs[row[0]] = dict(zip(JOB_TABLE_KEYS, row[1:]))
        jobs[row[0]]['id'] = row[0]
        jobs[row[0]]['tags'] = []

    job_ids = tuple(jobs.keys())
    tags_query = build_tags_query(tuple(jobs.keys()))
    cr.execute(tags_query, job_ids)
    tag_rows = cr.fetchall()

    for (job_id, tag_name) in tag_rows:
        jobs[job_id]["tags"].append({
            'name': tag_name,
            'url': f"/?{urlencode({'t': tag_name})}",
        })

    return {'count': jobs_count,
            'page_size': jobs_per_page,
            'current_page': page,
            'q': text_to_search or '',
            'page_range': page_range,
            'jobs': list(jobs.values())}


@app.route('/', methods=['GET'])
def index():
    text_to_search = request.args.get('q')
    page = int(request.args.get('p', "1")) - 1
    tag_name = request.args.get('t')
    context = get_job_listing_context(text_to_search, tag_name, page)
    return render_template('index.html', **context)


@app.route('/job/<pk>/', methods=['GET'])
def job_detail(pk: int):
    query = """
    select jobs.id, title, description, jobs.location, requirements,
           responsibilities, salary_range, hiring_process,
           companies.name as company_name
    from jobs
        inner join companies on jobs.company_id=companies.id
    where jobs.id == ?
    """
    cr.execute(query, [pk])
    job = dict(zip(JOB_TABLE_KEYS, cr.fetchone()[1:]))
    return render_template('detail.html', **job)


if __name__ == '__main__':
    app.config["DEBUG"] = True
    db.set_trace_callback(logging.debug)
    logging.basicConfig(level=logging.DEBUG)
    app.run()
