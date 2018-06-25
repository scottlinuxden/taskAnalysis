

# coding=utf-8
"""
Atlassian Jira module to read tasks based on JQL specification
"""
__author__ = 'Scott Davis'

from jira import JIRA
from dateutil.parser import parse
import sys
import time


def _date_string_to_datetime(date):
    date_time = None
    if date is not None:
        if date.strip() != '':
            # remove time zone offset
            date_time = parse((date[:date.rindex('-')]).strip())
            # date_time = parse(string.strip(date[:date.rindex('-')]))

    return date_time


def _time_to_complete_message(issue_key,
                              number_of_issues,
                              issue_being_processed,
                              execution_time):
    time_to_complete = ((float(number_of_issues - issue_being_processed)) * float(execution_time)) / 60.0
    if time_to_complete >= 1.0:
        message = "\rProcessing Issue: %s, Estimated Time to Complete: %  2.1f minutes" % \
                  (issue_key.rjust(10), time_to_complete)
    else:
        time_to_complete = (number_of_issues - issue_being_processed) * execution_time
        message = "\rProcessing Issue: %s, Estimated Time to Complete: %   d seconds" % \
                  (issue_key.rjust(10), time_to_complete)
    return message


class JiraProcessor:

    def __init__(self,
                 jira_cloud_url,
                 jira_login_username,
                 jira_login_password,
                 jira_unplanned_activity_field_name,
                 jira_epic_field_name,
                 jira_vacation_issue_type_name,
                 verbose=False):

        self.verbose = verbose
        self.jira_cloud_url = jira_cloud_url
        options = {
            'server': 'https://%s' % self.jira_cloud_url}
        self.jira = JIRA(options, basic_auth=(jira_login_username, jira_login_password))
        self.jira_unplanned_activity_field_name = jira_unplanned_activity_field_name
        self.jira_epic_field_name = jira_epic_field_name
        self.jira_vacation_issue_type_name = jira_vacation_issue_type_name

        # Get all projects viewable by anonymous users.
        self.projects = self.jira.projects()
        self.users_work_load = {}

    def get_access_token(self):

        import requests
        from oauthlib.oauth1 import SIGNATURE_RSA
        from requests_oauthlib import OAuth1Session
        from jira.client import JIRA

        # The Consumer Key created while setting up the "Incoming Authentication" in
        # JIRA for the Application Link.
        CONSUMER_KEY = ''

        # The contents of the rsa.pem file generated (the private RSA key)
        with open('/path/to/your/rsa/keys/rsa.pem') as f:
            RSA_KEY = f.read()

        # The URLs for the JIRA instance
        JIRA_SERVER = 'https://%s' % self.jira_cloud_url
        REQUEST_TOKEN_URL = JIRA_SERVER + '/plugins/servlet/oauth/request-token'
        AUTHORIZE_URL = JIRA_SERVER + '/plugins/servlet/oauth/authorize'
        ACCESS_TOKEN_URL = JIRA_SERVER + '/plugins/servlet/oauth/access-token'

        # Step 1: Get a request token

        oauth = OAuth1Session(CONSUMER_KEY, signature_type='auth_header',
                              signature_method=SIGNATURE_RSA, rsa_key=RSA_KEY)
        request_token = oauth.fetch_request_token(REQUEST_TOKEN_URL)

        print("STEP 1: GET REQUEST TOKEN")
        print("  oauth_token={}".format(request_token['oauth_token']))
        print("  oauth_token_secret={}".format(request_token['oauth_token_secret']))
        print("\n")

        # Step 2: Get the end-user's authorization

        print("STEP2: AUTHORIZATION")
        print("  Visit to the following URL to provide authorization:")
        print("  {}?oauth_token={}".format(AUTHORIZE_URL, request_token['oauth_token']))
        print("\n")

        while input("Press any key to continue..."):
            pass

        # Step 3: Get the access token

        access_token = oauth.fetch_access_token(ACCESS_TOKEN_URL)

        print("STEP2: GET ACCESS TOKEN")
        print("  oauth_token={}".format(access_token['oauth_token']))
        print("  oauth_token_secret={}".format(access_token['oauth_token_secret']))
        print("\n")

        # Now you can use the access tokens with the JIRA client. Hooray!

        jira = JIRA(options={'server': JIRA_SERVER}, oauth={
            'access_token': access_token['oauth_token'],
            'access_token_secret': access_token['oauth_token_secret'],
            'consumer_key': CONSUMER_KEY,
            'key_cert': RSA_KEY
        })

    def projects(self):
        return self.projects

    def task_work_logged(self):
        return self.users_work_load

    def tasks(self,
              normalize_assignee,
              jql='project in (EL, SUS, MEC, SOF, TEST) ORDER BY created DESC',
              include_work_log=False):

        issues = []
        start_at = 0
        max_results = 100
        results = {}
        last_number_of_fetched_issues = 0

        print('Jira Site %s Issue Retrieval' % self.jira_cloud_url)

        while True:

            issues = issues + self.jira.search_issues(jql_str=jql,
                                                      startAt=start_at,
                                                      maxResults=max_results,
                                                      validate_query=True,
                                                      fields=[],
                                                      expand=None,
                                                      json_result=None)

            if last_number_of_fetched_issues == len(issues):
                break

            sys.stdout.write("\rFetching %d issues at a time starting at issue %d" % (max_results, start_at))
            sys.stdout.flush()

            last_number_of_fetched_issues = len(issues)
            # print last_number_of_fetched_issues
            # print len(issues)

            start_at = start_at + max_results

        print()
        print('Total number of issues retrieved: %d' % len(issues))

        issue_number = 0
        for issue in issues:

            start_time = time.time()

            issue_number += 1

            if self.verbose:
                print(issue)
                for field_name in issue.raw['fields']:
                    print("Field:", field_name, "Value:", issue.raw['fields'][field_name])

            results[issue.key] = {}

            if issue.raw['fields']['assignee'] is not None:
                results[issue.key]['Assignee'] = normalize_assignee(issue.raw['fields']['assignee']['displayName'])
            else:
                results[issue.key]['Assignee'] = 'Unassigned'

            results[issue.key]['Description'] = None
            if issue.raw['fields']['description'] is not None:
                results[issue.key]['Description'] = issue.raw['fields']['description']

            results[issue.key]['Summary'] = None
            if issue.raw['fields']['summary'] is not None:
                results[issue.key]['Summary'] = issue.raw['fields']['summary']

            results[issue.key]['Issue Type'] = issue.raw['fields']['issuetype']['name']

            results[issue.key]['Resolution'] = None

            if issue.raw['fields']['resolution'] is not None:
                results[issue.key]['Resolution'] = issue.raw['fields']['resolution']['name']

            results[issue.key]['Created Date'] = _date_string_to_datetime(date=issue.raw['fields']['created'])

            results[issue.key]['Original Estimate'] = issue.raw['fields']['timeoriginalestimate']

            if results[issue.key]['Original Estimate'] is None:
                results[issue.key]['Original Estimate'] = 0

            results[issue.key]['Progress'] = issue.raw['fields']['progress']['progress']

            results[issue.key]['Remaining Estimate'] = issue.raw['fields']['timeestimate']

            if results[issue.key]['Remaining Estimate'] is None:
                results[issue.key]['Remaining Estimate'] = 0

            results[issue.key]['Time Spent'] = issue.raw['fields']['timespent']
            if results[issue.key]['Time Spent'] is None:
                results[issue.key]['Time Spent'] = 0

            if issue.raw['fields']['reporter'] is not None:
                results[issue.key]['Reporter'] = normalize_assignee(issue.raw['fields']['reporter']['displayName'])
            else:
                results[issue.key]['Reporter'] = 'Unassigned'

            results[issue.key]['Unplanned'] = False
            if issue.raw['fields'][self.jira_unplanned_activity_field_name] is not None:
                results[issue.key]['Unplanned'] = \
                    (issue.raw['fields'][self.jira_unplanned_activity_field_name][0]['value'] == 'Unplanned')

            if results[issue.key]['Issue Type'] == self.jira_vacation_issue_type_name:
                results[issue.key]['Unplanned'] = True

            results[issue.key]['Epic'] = issue.raw['fields'][self.jira_epic_field_name]

            results[issue.key]['Start Date'] = None
            results[issue.key]['End Date'] = None

            results[issue.key]['Problem'] = None

            results[issue.key]['Work Log'] = []

            log_no = 0

            if include_work_log:

                work_logs = self.jira.worklogs(issue.key)

                for work_log in work_logs:

                    log_no += 1

                    results[issue.key]['Work Log'].append(
                        {'Assignee': normalize_assignee(work_log.author.displayName),
                         'Created Date': _date_string_to_datetime(date=work_log.created),
                         'Time Spent': work_log.timeSpentSeconds})

                    if normalize_assignee(work_log.author.displayName) not in self.users_work_load:
                        self.users_work_load[normalize_assignee(work_log.author.displayName)] = \
                            {'total_logged_work': work_log.timeSpentSeconds}
                    else:
                        self.users_work_load[normalize_assignee(work_log.author.displayName)]['total_logged_work'] += \
                            work_log.timeSpentSeconds

            end_time = time.time()
            execution_time = end_time - start_time

            message = _time_to_complete_message(issue.key, len(issues), issue_number, execution_time)
            sys.stdout.write(message)
            sys.stdout.flush()

        return results
