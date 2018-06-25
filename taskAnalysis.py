# coding=utf-8
"""
Task Analysis processor of Smartsheet and Jira task, and Outlook Calendar Meetings
to arrive at all unplanned and planned activities that a project team is currently
involved in.
"""
__author__ = 'Scott Davis'

import csv
import string
from datetime import datetime
import BusinessHours
import glob2
import os
import io
import atlassian
import schedule
from datetime import timedelta
from dateutil.parser import parse
import sqlite3
import sqlite


# README
# Go into Jira and run the Query
# All Engineering Activites
# then export to CSV
# The file All Engineering Activities (JIRA).csv will be created
# Go into Smartsheet and select the following sheet
# 2018 Top Projects
# Export to Excel
# The file 2018 Top Projects.csv will be created
# Then run this script and the following files and output will be produced

# Vacation is recorded in Jira as follows:
#
# Issue type is set to vacation and then original estimate is set to the amount of vacation taken
# Reporter not assignee is the person taking the vacation.


def _escape_value(value):
    if isinstance(value, str):
        value = value.replace("'", "\'\'")
        return value
    else:
        return str(value)


def _format_date_to_yyyy_mm_dd(date):
    # remove any time HH:MM on date
    tokens = date.split(' ')
    tokens = tokens[0].split('/')
    date = '%s/%s/%s' % (tokens[2], tokens[0], tokens[1])
    return date


def _csv_row_to_string(data):
    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(data)
    return si.getvalue().strip('\r\n')


def _is_csv_row_empty(row):
    line = ''
    for column in row:
        line = '%s%s' % (line, column)

    if line.strip() == '':
        return True
    else:
        return False


def _date_string_to_datetime(date, date_format):
    date_time = None
    if date is not None:
        if date.strip() != '':
            date_time = datetime.strptime(date.strip(), date_format)
    return date_time


def _generic_date_string_to_date_time(date):
    date_time = None
    if date is not None:
        if date.strip() != '':
            # remove time zone offset
            date_time = parse(date.strip())

    return date_time


def _datetime_to_date_string(date_time):
    return date_time.strftime('%m/%d/%Y %H:%M')


def _date_string_for_first_day_in_current_year():
    date = '01/01/%d 00:00' % datetime.now().year
    return date


def _date_string_for_last_day_in_current_year():
    date = '12/31/%d 23:59' % datetime.now().year
    return date


def _file_modification_date(filename):
    t = os.path.getmtime(filename)
    return _datetime_to_date_string(datetime.fromtimestamp(t))


def _is_in_dictionary(value, dictionary):
    for key in dictionary.keys():
        if value.lower() == dictionary[key].lower():
            return True
    else:
        return False


class Processor:

    def __init__(self,
                 employee_info,
                 database_filename,
                 start_date,
                 end_date=None,
                 calendar_file_wildcard=None,
                 company_name=None,
                 jira_cloud_url=None,
                 jira_login_username=None,
                 jira_login_password=None,
                 jira_planned_task_departments=None,
                 jira_unplanned_task_departments=None,
                 jira_unplanned_activity_field_name=None,
                 jira_epic_field_name=None,
                 smartsheet_projects=None,
                 smartsheet_access_token=None,
                 update_smartsheet_progress=None,
                 holidays_file=None,
                 jira_vacation_issue_type_name='Vacation',
                 mail_server_domain_names=None,
                 verbose=False):

        self.company_name = company_name
        self.jira_cloud_url = jira_cloud_url
        self.jira_login_username = jira_login_username
        self.jira_login_password = jira_login_password
        self.jira_planned_task_departments = jira_planned_task_departments
        self.jira_unplanned_task_departments = jira_unplanned_task_departments
        self.jira_unplanned_activity_field_name = jira_unplanned_activity_field_name
        self.jira_epic_field_name = jira_epic_field_name
        self.jira_vacation_issue_type_name = jira_vacation_issue_type_name

        self.holidays_file = holidays_file
        self.employee_info = employee_info
        self.verbose = verbose
        self.tasks = {}
        self.calendar_file_wildcard = calendar_file_wildcard
        self.mail_server_domain_names = mail_server_domain_names

        self.update_smartsheet_progress = update_smartsheet_progress
        self.smartsheet_projects = smartsheet_projects

        self.employees = {}
        self.employee_names = self.employee_info.keys()
        self.employee_names = sorted(self.employee_names, key=lambda x: x.split(" ")[-1])
        self.smartsheet_access_token = smartsheet_access_token

        self.workDayTime = [8, 16]
        self.plannedEmployees = []
        self.unplannedEmployees = []

        self.seconds_in_minute = 60
        self.seconds_in_hour = 60 * self.seconds_in_minute
        self.seconds_in_day = 8 * self.seconds_in_hour
        self.seconds_in_week = 5 * self.seconds_in_day

        self.database_filename = database_filename

        # Jira is assigning some names that need aliased
        self.employee_aliases = {}

        for employee_name in self.employee_info.keys():
            if self.employee_info[employee_name]['aliases'] is not None:
                for alias_name in self.employee_info[employee_name]['aliases']:
                    self.employee_aliases[alias_name] = employee_name

        self._add_email_aliases(self.mail_server_domain_names)

        self.holiday_date_format = '%d-%m-%Y'
        self.jira_date_format = '%m/%d/%Y %H:%M'
        self.business_hours_date_format = '%m/%d/%Y %H:%M'
        self.smartsheet_date_format = '%m/%d/%y'

        # set weekend day numbers to Saturday and Sunday
        self.weekends = [6, 7]

        self.start_date, self.end_date = self._set_date_range(start_date, end_date)

        self.jira_unplanned_task_departments.update(self.jira_planned_task_departments)

        self.unplanned_activity = "Unplanned"

        self.jira_field_names = ['Epic',
                                 'Issue Key',
                                 'Summary',
                                 'Assignee',
                                 'Issue Type',
                                 'Unplanned',
                                 'Resolution',
                                 'Created Date',
                                 'Reporter',
                                 'Description',
                                 'Original Estimate',
                                 'Remaining Estimate',
                                 'Time Spent',
                                 'Work Log']

        self.task_output_format = self.jira_field_names
        self.task_output_format.append('Start Date')
        self.task_output_format.append('End Date')
        self.task_output_format.append('Progress')
        self.task_output_format.append('Problem')

        self.task_log_output_format = ['Issue Key',
                                       'Assignee',
                                       'Created Date',
                                       'Time Spent']

        self.task_db_row = {}
        for column_name in self.task_output_format:
            self.task_db_row[column_name] = None

        self.task_date_fields = ['Start Date',
                                 'End Date',
                                 'Created Date']

        self.task_time_fields = ['Remaining Estimate',
                                 'Original Estimate',
                                 'Time Spent',
                                 'Progress']

        self.task_name_fields = ['Assignee',
                                 'Reporter']

        self.scheduled_projects = []

        self.total_planned_hours = 0
        self.total_unplanned_hours = 0
        self.total_holiday_hours = 0
        self.total_vacation_hours = 0
        self.lastInputFileDate = None
        self.total_meeting_hours = 0

        if self.jira_cloud_url is not None:

            self.jira = atlassian.JiraProcessor(jira_cloud_url=self.jira_cloud_url,
                                                jira_login_username=self.jira_login_username,
                                                jira_login_password=self.jira_login_password,
                                                jira_unplanned_activity_field_name=self.jira_unplanned_activity_field_name,
                                                jira_epic_field_name=self.jira_epic_field_name,
                                                jira_vacation_issue_type_name=self.jira_vacation_issue_type_name,
                                                verbose=self.verbose)

            projects_name_string = ', '.join(self.jira_planned_task_departments.values())
            projects_name_string += ', ' + ', '.join(self.jira_unplanned_task_departments.values())

            self.tasks = self.jira.tasks(
                normalize_assignee=self._normalize_name,
                jql="project in (%s) and createdDate >= '%s' and createdDate <= '%s' ORDER BY created DESC" %
                    (projects_name_string,
                     _format_date_to_yyyy_mm_dd(self.start_date),
                     _format_date_to_yyyy_mm_dd(self.end_date)),
                include_work_log=True)

            self.jira_unplanned_task_departments['meeting'] = 'MEET'

            self.task_work_logged = self.jira.task_work_logged()

            self.smartsheet = schedule.SmartsheetProcessor(company_name=self.company_name,
                                                           access_token=self.smartsheet_access_token,
                                                           smartsheet_projects=self.smartsheet_projects,
                                                           normalize_assignee=self._normalize_name,
                                                           update_sheet_progress=self.update_smartsheet_progress)

            self.smartsheet_tasks = self.smartsheet.scheduled_tasks()
            self.smartsheet_task_issues = self.smartsheet.scheduled_task_issues()

            # merge in smartsheet_task data into exising jira tasks
            # there is no way a smartsheet task can exist without it being in jira
            for issue_key in self.smartsheet_tasks.keys():
                # if the issue is in the jira tasks we are merging into then
                # set start and end date and make sure unplanned gets set to false
                # which indicates this is a planned task
                # ignore tasks in smartsheet that are not already in task list since they are not in jira
                if issue_key in self.tasks:
                    self._insert_or_update_task_in_memory(issue_key=issue_key,
                                                          unplanned=False,
                                                          start_date_time=self.smartsheet_tasks[issue_key]['Start Date'],
                                                          end_date_time=self.smartsheet_tasks[issue_key]['End Date'])

            if self.calendar_file_wildcard is not None:

                self.calendar_header = {}
                self.calendar_filename = 'calendars.csv'

                self._add_assignee_to_calendar_files(self.calendar_file_wildcard)

                self.calendar_file_wildcard = '*_calendar_with_assignee.csv'

                self.calendar_file_mod_date = self._build_input_from_files(file_wildcard=self.calendar_file_wildcard,
                                                                           output_filename=self.calendar_filename,
                                                                           header_columns=['Subject',
                                                                                           'Start Date',
                                                                                           'Start Time',
                                                                                           'End Date'])

                self._load_outlook_calendars()

            self._load_tasks_into_db()

        self.task_db = sqlite.Database(self.database_filename, 'tasks')
        self.task_log_db = sqlite.Database(self.database_filename, 'task_logs')

    def _select_build_columns(self):

        columns = 'issue_key'
        for column_name in self.task_output_format:
            if column_name != 'issue_key' and column_name != 'work_log':
                columns = '%s, %s' % (columns, column_name.replace(' ', '_').lower())
        return columns

    def _add_email_aliases(self, mailserver_domain_names):

        if mailserver_domain_names is not None:
            # copy aliases specified on call to employee alias lookup table
            for employee_name in self.employee_info.keys():
                employee_email_name = employee_name.split(' ')
                for domain_name in mailserver_domain_names:
                    email_alias = '%s.%s@%s' % (employee_email_name[0].lower(), employee_email_name[1].lower(),
                                                domain_name.lower())
                    if self.employee_info[employee_name]['aliases'] is not None:
                        self.employee_info[employee_name]['aliases'].append(email_alias)
                    else:
                        self.employee_info[employee_name]['aliases'] = [email_alias]
                    self.employee_aliases[email_alias] = employee_name

    def _select_task_from_db(self, issue_key):

        return self.task_db.select(where={'Issue Key': issue_key})

    def _insert_task_logs_into_db(self, issue_key, cursor):

        # print(self.tasks[issue_key])

        for log_entry in self.tasks[issue_key]['Work Log']:

            sql_command = "INSERT INTO task_logs(issue_key"

            for column_name in log_entry.keys():

                if column_name != 'Issue Key':
                    column_name_reformatted = column_name.lower().replace(' ', '_')

                    column_specifier = ', {cn}'.format(cn=column_name_reformatted)
                    sql_command = '%s%s' % (sql_command, column_specifier)

            sql_command = "%s) VALUES ('%s'" % (sql_command, _escape_value(issue_key))

            for column_name in log_entry.keys():

                if column_name != 'Issue Key':

                    column_value = "NULL"

                    if column_name in self.task_time_fields:
                        if log_entry[column_name] is not None:
                            column_value = "%1.2f" % (log_entry[column_name])

                    elif column_name in self.task_name_fields:
                        if log_entry[column_name] is not None:
                            column_value = "'%s'" % (
                                _escape_value(self._normalize_name(log_entry[column_name])))

                    elif column_name in self.task_date_fields:
                        if log_entry[column_name] is not None:
                            column_value = "'%s'" % (
                                _escape_value(_datetime_to_date_string(log_entry[column_name])))

                    else:
                        if log_entry[column_name] is not None:
                            column_value = "'%s'" % (_escape_value(log_entry[column_name]))

                    sql_command = '%s, %s' % (sql_command, column_value)

            sql_command = '%s)' % sql_command

            try:
                cursor.execute(sql_command)
            except sqlite3.IntegrityError:
                print('ERROR: Table TASK_LOGS')

    def _insert_task_into_db(self, issue_key, cursor):

        sql_command = "INSERT INTO tasks(issue_key"

        for column_name in self.tasks[issue_key].keys():
            if column_name != 'Issue Key' and column_name != 'Work Log':
                column_name_reformatted = column_name.lower().replace(' ', '_')

                column_specifier = ', {cn}'.format(cn=column_name_reformatted)
                sql_command = '%s%s' % (sql_command, column_specifier)

        sql_command = "%s) VALUES ('%s'" % (sql_command, _escape_value(issue_key))

        for column_name in self.tasks[issue_key].keys():

            if column_name != 'Issue Key' and column_name != 'Work Log':

                column_value = "NULL"

                if column_name in self.task_time_fields:
                    if self.tasks[issue_key][column_name] is not None:
                        column_value = "%1.2f" % (self.tasks[issue_key][column_name])

                elif column_name in self.task_name_fields:
                    if self.tasks[issue_key][column_name] is not None:
                        column_value = "'%s'" % (
                            _escape_value(self._normalize_name(self.tasks[issue_key][column_name])))

                elif column_name in self.task_date_fields:
                    if self.tasks[issue_key][column_name] is not None:
                        column_value = "'%s'" % (
                            _escape_value(_datetime_to_date_string(self.tasks[issue_key][column_name])))

                elif column_name == 'Unplanned':
                    if self.tasks[issue_key][column_name]:
                        column_value = "1"
                    else:
                        column_value = "0"

                else:
                    if self.tasks[issue_key][column_name] is not None:
                        column_value = "'%s'" % (_escape_value(self.tasks[issue_key][column_name]))

                sql_command = '%s, %s' % (sql_command, column_value)

        sql_command = '%s)' % sql_command

        try:
            cursor.execute(sql_command)
        except sqlite3.IntegrityError:
            print('ERROR: Table TASKS, ID already exists in PRIMARY KEY column Issue Key')

        self._insert_task_logs_into_db(issue_key, cursor)

    def _load_tasks_into_db(self):

        connection = sqlite3.connect(self.database_filename)

        cursor = connection.cursor()

        self._create_db(cursor)

        for issue_key in self.tasks:
            self._insert_task_into_db(issue_key, cursor)

        connection.commit()
        connection.close()

    def _is_vacation(self, issue_type):

        if issue_type == self.jira_vacation_issue_type_name:
            return True
        else:
            return False

    def _create_db(self, cursor):

        sql_command = 'DROP TABLE tasks'

        try:
            cursor.execute(sql_command)
        except:
            pass

        sql_command = 'CREATE TABLE tasks (issue_key TEXT PRIMARY KEY NOT NULL'

        for column_name in self.task_output_format:

            if column_name != 'Issue Key' and column_name != 'Work Log':
                add = ''
                field_type = 'TEXT'

                if column_name in self.task_time_fields:
                    field_type = 'REAL'
                elif column_name == 'Unplanned':
                    field_type = 'INTEGER'

                column_name_reformatted = column_name.lower().replace(' ', '_')
                column_specifier = ', {cn} {ft} {add}'.format(cn=column_name_reformatted, ft=field_type, add=add)
                sql_command = '%s%s' % (sql_command, column_specifier)

        sql_command = '%s)' % sql_command

        cursor.execute(sql_command)

        sql_command = 'DROP TABLE task_logs'

        try:
            cursor.execute(sql_command)
        except:
            pass

        sql_command = 'CREATE TABLE task_logs (issue_key TEXT NOT NULL'

        for column_name in self.task_log_output_format:

            if column_name != 'Issue Key':
                add = ''
                field_type = 'TEXT'

                if column_name in self.task_time_fields:
                    field_type = 'REAL'

                column_name_reformatted = column_name.lower().replace(' ', '_')
                column_specifier = ', {cn} {ft} {add}'.format(cn=column_name_reformatted, ft=field_type, add=add)
                sql_command = '%s%s' % (sql_command, column_specifier)

        sql_command = '%s)' % sql_command

        cursor.execute(sql_command)

    def _dump_tasks_to_file(self, filename):
        task_file = open(filename, 'w')

        task_writer = csv.writer(task_file,
                                 delimiter=',',
                                 quotechar='"',
                                 quoting=csv.QUOTE_MINIMAL)

        columns = []
        for column_header in self.task_output_format:
            if column_header != 'Issue Key' and column_header != 'Work Log':
                columns.append(column_header)
        task_writer.writerow(['Issue Key'] + columns)

        for issue_key in self.tasks:
            columns = []
            for task_key in self.task_output_format:
                if task_key != 'Issue Key' and task_key != 'Work Log':
                    columns.append(self.tasks[issue_key][task_key])
            task_writer.writerow([issue_key] + columns)

        task_file.close()

    def _get_next_weekday_from_date(self, date, date_format):
        if not self._is_date_a_week_day(date=date, date_format=date_format):
            date_time = _date_string_to_datetime(date, date_format)
            date_time = date_time + timedelta(days=7 - date_time.weekday())
            date = _datetime_to_date_string(date_time)
        return date

    def _add_assignee_to_calendar_files(self, file_wildcard):
        filenames = glob2.glob(file_wildcard)

        for filename in filenames:

            calendar_file = open(file=filename, mode='r', errors='ignore')

            csv_reader = csv.reader(calendar_file,
                                    delimiter=',',
                                    quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)

            first_data_row, header = self._get_calendar_column_headers(csv_reader, ignore_assignee_column=True)

            tokens = filename.split('.')  # split filename at extension
            calendar_file_with_assignee_filename = '%s_with_assignee.%s' % (tokens[0], tokens[1])

            if os.path.exists(calendar_file_with_assignee_filename):
                os.remove(calendar_file_with_assignee_filename)

            calendar_file_with_assignee = open(calendar_file_with_assignee_filename, 'w')

            csv_writer = csv.writer(calendar_file_with_assignee,
                                    delimiter=',',
                                    quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)

            tokens = tokens[0].split('_')
            assignee = string.capwords('%s %s' % (tokens[0], tokens[1]))

            row_number = 0

            # go back to beginning of file to re-read header
            calendar_file.seek(0)

            for row in csv_reader:

                if row_number > first_data_row:

                    if not _is_csv_row_empty(row):

                        if self.verbose:
                            print(row)

                        csv_writer.writerow([assignee] + row)

                else:
                    csv_writer.writerow(['Assignee'] + row)

                row_number = row_number + 1

            calendar_file_with_assignee.close()
            calendar_file.close()

    def _build_input_from_files(self, file_wildcard, output_filename, header_columns, set_input_file_mod_date=True):

        filenames = glob2.glob(file_wildcard)  # list of all wild_card files in the directory
        f = open(output_filename, 'w')
        file_no = 0

        # check if we need to set the
        if set_input_file_mod_date:
            self.lastInputFileDate = _file_modification_date(filenames[0])

        modification_date = _file_modification_date(output_filename)

        for file in filenames:
            line_no = 0
            file_no = file_no + 1
            infile = open(file, 'r')
            for line in infile:
                line_no = line_no + 1
                # if second or N file being processed check for file header row and blank line
                # which needs removed
                # if line is not blank
                if self.verbose:
                    print(line)
                if line.strip() != '':
                    # split the line into columns to check content of line
                    columns = line.split(',')
                    if len(columns) >= len(header_columns):
                        # if second to N file
                        if file_no > 1:
                            # check if line is a header line
                            header_row = True
                            i = 0
                            for header_column in header_columns:
                                if columns[i] not in header_column:
                                    header_row = False
                                    break
                                i = i + 1

                            if header_row:
                                # skip writing line since this file 2 thru n being concatenated and it should
                                # not be written to output
                                pass
                            else:
                                f.write(line)
                        else:
                            f.write(line)
                    else:
                        f.write(line)

            infile.close()
        f.close()

        return modification_date

    def _initialize_employee_planned(self):

        for current_employee in self.employee_names:
            # if self.employees.has_key(current_employee):
            if current_employee in list(self.employees):
                self.employees[current_employee]['planned_work_time'] = 0
            else:
                self.employees[current_employee] = {'planned_work_time': 0, 'unplanned_work_time': 0,
                                                    'vacation_time': 0, 'meeting_time': 0}

    def _initialize_employee_unplanned(self):

        for current_employee in self.employee_names:
            if current_employee in list(self.employees):
                self.employees[current_employee]['unplanned_work_time'] = 0
                self.employees[current_employee]['vacation_time'] = 0
                self.employees[current_employee]['meeting_time'] = 0
            else:
                self.employees[current_employee] = {'planned_work_time': 0, 'unplanned_work_time': 0,
                                                    'vacation_time': 0, 'meeting_time': 0}

    def _initialize_employee_data(self):

        for current_employee in self.employee_names:
            self.employees[current_employee] = {'unplanned_work_time': 0, 'planned_work_time': 0, 'vacation_time': 0,
                                                'meeting_time': 0}

    def _insert_or_update_task_in_memory(self,
                                         issue_key,
                                         unplanned,
                                         assignee=None,
                                         epic=None,
                                         summary=None,
                                         original_estimate=None,
                                         remaining_estimate=None,
                                         time_spent=None,
                                         issue_type=None,
                                         reporter=None,
                                         resolution=None,
                                         description=None,
                                         start_date_time=None,
                                         end_date_time=None,
                                         created_date_time=None,
                                         progress=None,
                                         problem=None,
                                         work_log=[]):

        if issue_key in self.tasks:

            # print 'Issue: %s, Task: %s' % (issue_key, self.tasks[issue_key])

            # only change values of existing tasks if they have not already been set
            if self.tasks[issue_key]['Issue Type'] is None:
                self.tasks[issue_key]['Issue Type'] = issue_type

            # if the task should be marked planned then potentially set an unplanned task
            # as planned.
            if not unplanned:
                self.tasks[issue_key]['Unplanned'] = unplanned

            if self.tasks[issue_key]['Assignee'] is None:
                self.tasks[issue_key]['Assignee'] = assignee

            if self.tasks[issue_key]['Epic'] is None:
                self.tasks[issue_key]['Epic'] = epic

            if self.tasks[issue_key]['Summary'] is None:
                self.tasks[issue_key]['Summary'] = summary

            if self.tasks[issue_key]['Original Estimate'] is None:
                self.tasks[issue_key]['Original Estimate'] = original_estimate

            if self.tasks[issue_key]['Remaining Estimate'] is None:
                self.tasks[issue_key]['Remaining Estimate'] = original_estimate

            if self.tasks[issue_key]['Time Spent'] is None:
                self.tasks[issue_key]['Time Spent'] = time_spent

            if self.tasks[issue_key]['Reporter'] is None:
                self.tasks[issue_key]['Reporter'] = reporter

            if self.tasks[issue_key]['Resolution'] is None:
                self.tasks[issue_key]['Resolution'] = resolution

            if self.tasks[issue_key]['Description'] is None:
                self.tasks[issue_key]['Description'] = description

            if self.tasks[issue_key]['Start Date'] is None:
                self.tasks[issue_key]['Start Date'] = start_date_time

            if self.tasks[issue_key]['End Date'] is None:
                self.tasks[issue_key]['End Date'] = end_date_time

            # if tasks already exists and this is an update do not update created
            # date time unless is is null
            if self.tasks[issue_key]['Created Date'] is None:
                self.tasks[issue_key]['Created Date'] = created_date_time

            if self.tasks[issue_key]['Progress'] is None:
                self.tasks[issue_key]['Progress'] = progress

            if self.tasks[issue_key]['Problem'] is None:
                self.tasks[issue_key]['Problem'] = problem

            self.tasks[issue_key]['Work Log'] = work_log

        else:
            # print 'Issue: %s not found in Jira tasks but is found in Smartsheet' % (issue_key)
            self.tasks[issue_key] = {'Issue Type': issue_type,
                                     'Unplanned': unplanned,
                                     'Assignee': assignee,
                                     'Reporter': reporter,
                                     'Resolution': resolution,
                                     'Start Date': start_date_time,
                                     'End Date': end_date_time,
                                     'Created Date': created_date_time,
                                     'Epic': epic,
                                     'Summary': summary,
                                     'Description': description,
                                     'Original Estimate': original_estimate,
                                     'Remaining Estimate': remaining_estimate,
                                     'Time Spent': time_spent,
                                     'Progress': progress,
                                     'Problem': problem,
                                     'Work Log': []}

    def _get_calendar_column_headers(self, cvs_reader, ignore_assignee_column=False):
        calendar_header = {}
        first_data_row = 0
        row_number = 0

        for row in cvs_reader:
            if len(row) != 0:
                if not ignore_assignee_column:
                    calendar_header['Assignee'] = row.index('Assignee')
                calendar_header['Subject'] = row.index('Subject')
                calendar_header['Start Date'] = row.index('Start Date')
                calendar_header['Start Time'] = row.index('Start Time')
                calendar_header['End Date'] = row.index('End Date')
                calendar_header['End Time'] = row.index('End Time')
                calendar_header['Meeting Organizer'] = row.index('Meeting Organizer')
                calendar_header['Required Attendees'] = row.index('Required Attendees')
                calendar_header['Description'] = row.index('Description')
                calendar_header['Categories'] = row.index('Categories')
                calendar_header['All Day Event'] = row.index('All day event')
                calendar_header['Private'] = row.index('Private')
                calendar_header['Location'] = row.index('Location')
                first_data_row = row_number
                break
            else:
                row_number = row_number + 1

        return first_data_row, calendar_header

    def _get_jira_column_headers(self, csv_reader):
        jira_header = {}
        first_data_row = 0
        row_number = 0

        for row in csv_reader:
            if len(row) != 0:
                jira_header['Assignee'] = row.index('Assignee')
                jira_header['Summary'] = row.index('Summary')
                jira_header['Description'] = row.index('Description')
                jira_header['Issue Type'] = row.index('Issue Type')
                jira_header['Issue Key'] = row.index('Issue key')
                jira_header['Original Estimate'] = row.index('Original Estimate')
                jira_header['Remaining Estimate'] = row.index('Remaining Estimate')
                jira_header['Time Spent'] = row.index('Time Spent')
                jira_header['Created Date'] = row.index('Created')
                jira_header['Reporter'] = row.index('Reporter')
                jira_header['Resolution'] = row.index('Resolution')
                jira_header['Epic'] = row.index('Custom field (Epic Link)')
                jira_header['Unplanned'] = row.index('Custom field (Unplanned Activity?)')
                first_data_row = row_number
                break
            else:
                row_number = row_number + 1

        return first_data_row, jira_header

    def _get_smartsheet_column_headers(self, csv_reader):
        smartsheet_header = {}
        first_data_row = 0
        row_number = 0
        for row in csv_reader:
            if len(row) != 0:
                smartsheet_header['Issue Key'] = row.index('\xef\xbb\xbfIssue Key')
                smartsheet_header['Summary'] = row.index('Summary')
                smartsheet_header['Issue Type'] = row.index('Issue Type')
                smartsheet_header['Epic'] = row.index('Epic Link')
                smartsheet_header['Start Date'] = row.index('Start Date')
                smartsheet_header['End Date'] = row.index('End Date')
                smartsheet_header['Assignee'] = row.index('Assignee')
                smartsheet_header['Original Time Estimated'] = row.index('Original Time Estimated')
                smartsheet_header['Remaining Time Estimated'] = row.index('Remaining Time Estimated')
                smartsheet_header['Time Spent'] = row.index('Time Spent')
                smartsheet_header['Progress'] = row.index('Progress')
                first_data_row = row_number
                break
            else:
                row_number = row_number + 1

        return first_data_row, smartsheet_header

    def _normalize_name(self, assignee):
        try:
            if assignee in self.employee_names:
                return assignee
            elif string.capwords(assignee) in self.employee_names:
                return string.capwords(assignee)
            elif string.capwords(assignee.replace('.', ' ')) in self.employee_names:
                return string.capwords(assignee.replace('.', ' '))
            elif string.capwords(assignee.replace('.', ' ')) in list(self.employee_aliases):
                return self.employee_aliases[string.capwords(assignee.replace('.', ' '))]
            elif assignee in self.employee_aliases:
                return self.employee_aliases[assignee]
            else:
                return assignee
        except:
            return assignee

    def is_unplanned(self, issue_key):
        outcome = False
        if self.tasks[issue_key]['Unplanned']:
            outcome = True
        elif self.tasks[issue_key]['Issue Type'] == self.jira_vacation_issue_type_name:
            outcome = True
        elif self.tasks[issue_key]['Issue Type'] == 'Meeting':
            outcome = True
        return outcome

    def _is_date_time_during_working_hours(self, date_time):

        date_in_range = False

        if date_time is not None:
            if self._is_datetime_week_day(date_time):
                if self.workDayTime[0] <= date_time.hour <= self.workDayTime[1] + 1:
                    date_in_range = True

        return date_in_range

    def _is_planned_task_dept(self, issue_key):
        tokens = issue_key.split('-')
        return _is_in_dictionary(tokens[0], self.jira_planned_task_departments)

    def _is_unplanned_task_dept(self, issue_key):
        tokens = issue_key.split('-')
        return _is_in_dictionary(tokens[0], self.jira_unplanned_task_departments)

    def _is_date_a_week_day(self, date, date_format):
        date_time = _date_string_to_datetime(date, date_format)
        if date_time.weekday() not in self.weekends:
            return True
        else:
            return False

    def _is_datetime_week_day(self, date_time):
        if date_time.weekday() not in self.weekends:
            return True
        else:
            return False

    def date_string_in_period(self, date, date_format, start_date, end_date=None):

        # An empty string date is counted as an immediate task and is by
        # default included in range
        date_in_range = True

        start_date_time = _date_string_to_datetime(start_date, self.business_hours_date_format)
        if end_date is not None:
            end_date_time = _date_string_to_datetime(end_date, self.business_hours_date_format)
        else:
            end_date_time = datetime.now()

        date_time = _date_string_to_datetime(date, date_format)

        if date_time is not None:
            if date_time < start_date_time:
                date_in_range = False & date_in_range

            if date_time > end_date_time:
                date_in_range = False & date_in_range

        return date_in_range

    def _is_datetime_in_period(self, date_time, start_date, end_date=None):

        if date_time is not None:
            # An empty string date is counted as an immediate task and is by
            # default included in range
            date_in_range = True

            start_date_time = _date_string_to_datetime(start_date, self.business_hours_date_format)
            if end_date is not None:
                end_date_time = _date_string_to_datetime(end_date, self.business_hours_date_format)
            else:
                end_date_time = datetime.now()

            if date_time is not None:
                if date_time < start_date_time:
                    date_in_range = False & date_in_range

                if date_time > end_date_time:
                    date_in_range = False & date_in_range
        else:
            date_in_range = False

        return date_in_range

    def holiday_hours_per_employee(self, start_date, end_date=None):

        hours = 0
        f = open(file=self.holidays_file, mode='r', errors='ignore')
        fdata = f.read()
        definedholidays = fdata.split()
        f.close()
        # exclude any holidays that have been marked in the companies academic year
        for definedholiday in definedholidays:
            # day , month , year = definedholiday.split('-')
            # holidate = datetime.datetime(int(year) , int(month) , int(day), 0, 0, 0)
            if self.date_string_in_period(definedholiday, self.holiday_date_format, start_date, end_date):
                hours = hours + 8
        return hours

    def holiday_hours_for_all_employees_in_period(self, start_date, end_date):
        # two floating holidays are recorded as vacation
        return self.holiday_hours_per_employee(start_date, end_date) * len(self.employee_names)

    def _load_outlook_calendars(self):

        calendar_file = open(file=self.calendar_filename, mode='r', errors='ignore')

        csv_reader = csv.reader(calendar_file,
                                delimiter=',',
                                quotechar='"',
                                quoting=csv.QUOTE_MINIMAL)

        first_data_row, header = self._get_calendar_column_headers(csv_reader)

        row_number = 0
        entry_no = 0

        for row in csv_reader:

            if row_number >= first_data_row:

                if not _is_csv_row_empty(row):

                    if self.verbose:
                        print(row)

                    # if this is a calendar holiday entry ignore
                    if row[header['Categories']].strip() != 'Holiday':
                        # private meetings do not get recorded
                        if row[header['Private']].strip() != 'TRUE':
                            # ignore any agile meetings
                            if "agile" not in row[header['Subject']].lower().strip():
                                # ignore all day events
                                if row[header['All Day Event']].strip() != 'TRUE':
                                    # ignore cancelled meetings
                                    if 'Canceled' not in row[header['Subject']]:
                                        assignee = self._normalize_name(row[header['Assignee']])
                                        # get data from calendar
                                        subject = row[header['Subject']]
                                        organizer = row[header['Meeting Organizer']]
                                        description = row[header['Description']]
                                        start_date_time = _generic_date_string_to_date_time(
                                            row[header['Start Date']] + ' ' + row[header['Start Time']])
                                        end_date_time = _generic_date_string_to_date_time(
                                            row[header['End Date']] + ' ' + row[header['End Time']])
                                        time_spent = (end_date_time - start_date_time).total_seconds()

                                        entry_no = entry_no + 1

                                        if _date_string_to_datetime(self.start_date,
                                                                    self.business_hours_date_format) <= \
                                                start_date_time <= \
                                                _date_string_to_datetime(self.end_date,
                                                                         self.business_hours_date_format):
                                            self._insert_or_update_task_in_memory(
                                                issue_key='%s-%d' % (self.jira_unplanned_task_departments['meeting'],
                                                                     entry_no),
                                                unplanned=True,
                                                issue_type='Meeting',
                                                summary=subject,
                                                assignee=assignee,
                                                start_date_time=start_date_time,
                                                end_date_time=end_date_time,
                                                created_date_time=start_date_time,
                                                description=description,
                                                original_estimate=time_spent,
                                                reporter=organizer,
                                                time_spent=time_spent)

            row_number = row_number + 1

        calendar_file.close()

    def _load_all_tasks_from_csv_files(self,
                                       task_type,
                                       smartsheet_filename,
                                       smartsheet_date_format,
                                       jira_filename,
                                       jira_date_format):

        # Planned information is found in Smartsheet that has task designated start and end dates
        # Jira does not have the notion of start and end dates.  Jira planning is driven by sprints.

        unplanned = None
        first_data_row = 0
        csv_reader = None
        task_file = None

        if task_type == 'smartsheet':

            # open smartsheet data file
            task_file = open(file=smartsheet_filename, mode='r', errors='ignore')

            csv_reader = csv.reader(task_file,
                                    delimiter=',',
                                    quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)

            first_data_row, header = self._get_smartsheet_column_headers(csv_reader)

        elif task_type == 'jira':

            # open the input jira file
            task_file = open(file=jira_filename, mode='r', errors='ignore')

            csv_reader = csv.reader(task_file,
                                    delimiter=',',
                                    quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)

            # assigne column headers to get task data
            first_data_row, header = self._get_jira_column_headers(csv_reader)

        row_number = 0

        for row in csv_reader:

            if row_number >= first_data_row:

                if not _is_csv_row_empty(row):

                    # set all defaults
                    start_date_time = end_date_time = created_date_time = None
                    resolution = description = reporter = None
                    original_estimate = time_spent = 0

                    if self.verbose:
                        print(row)

                    # set assignee from task
                    assignee = row[header['Assignee']]
                    assignee = self._normalize_name(assignee)

                    # get data from task
                    issue_key = row[header['Issue Key']]
                    issue_type = row[header['Issue Type']]

                    epic = row[header['Epic']]

                    if epic.strip() == '':
                        epic = None

                    summary = row[header['Summary']]
                    # progress = row[self.smartsheet_header['Progress']]

                    if task_type == 'smartsheet':

                        start_date_time_string = row[header['Start Date']]
                        start_date_time = _date_string_to_datetime(start_date_time_string,
                                                                   smartsheet_date_format)

                        end_date_time_string = row[header['End Date']]
                        end_date_time = _date_string_to_datetime(end_date_time_string,
                                                                 smartsheet_date_format)

                        original_estimate_string = row[header['Original Time Estimated']]
                        time_spent_string = row[header['Time Spent']]

                        unplanned = False

                        # set time estimates
                        try:
                            original_estimate = schedule.time_to_seconds(time_value=original_estimate_string)
                        except:
                            original_estimate = 0

                        try:
                            time_spent = schedule.time_to_seconds(time_value=time_spent_string)
                        except:
                            time_spent = 0

                    elif task_type == 'jira':

                        try:
                            unplanned = self.is_unplanned(row[header['Unplanned']])
                        except:
                            # if we get an exception here it is because
                            # the record pre-dates when unplanned activity was added
                            # as custom field to jira so exit processing this task
                            continue

                        resolution = row[header['Resolution']]

                        description = row[header['Description']]

                        reporter = row[header['Reporter']]
                        reporter = self._normalize_name(reporter)

                        created_date_time = _date_string_to_datetime(row[header['Created Date']],
                                                                     jira_date_format)

                        # set time estimates
                        try:
                            original_estimate = int(row[header['Original Estimate']])
                        except:
                            original_estimate = 0

                        try:
                            time_spent = int(row[header['Time Spent']])
                        except:
                            time_spent = original_estimate

                    self._insert_or_update_task_in_memory(issue_key=issue_key,
                                                          unplanned=unplanned,
                                                          issue_type=issue_type,
                                                          assignee=assignee,
                                                          start_date_time=start_date_time,
                                                          end_date_time=end_date_time,
                                                          resolution=resolution,
                                                          created_date_time=created_date_time,
                                                          description=description,
                                                          reporter=reporter,
                                                          epic=epic,
                                                          summary=summary,
                                                          original_estimate=original_estimate,
                                                          time_spent=time_spent)

            row_number = row_number + 1

        task_file.close()
        return csv_reader

    # determine if task is a planned task in smartsheet
    def is_task_in_schedule(self, issue_key):

        # if the task is planned which means not a meeting, task unplanned, or vacation task
        if not self.is_unplanned(issue_key):

            # check and see if it is also in a smartsheet schedule
            if issue_key in self.smartsheet_task_issues:

                # determine if the task has a start and end date
                if self.tasks[issue_key]['Start Date'] is not None and self.tasks[issue_key]['End Date'] is not None:

                    # if it does it is a planned task to be executed or is already being executed
                    outcome = True

                # task does not have any start or end date
                else:

                    # task is a planned Jira task and part of a scheduled project in smartsheet
                    # that was not originally planned in the schedule but is now a planned item that was worked
                    # in that it does not have a start and end date
                    self._process_task_problem(issue_key, 'Info: unplanned task found in schedule')

                    outcome = True

            # task is not in a Smartsheet schedule
            else:

                # Planned activity outside of a planned project in smartsheet schedule
                outcome = True
                # record that we found a Jira task that is marked planned but not recorded in a schedule
                self._process_task_problem(issue_key, 'Info: planned task that is not found in a schedule')

        # task explicitly marked as unplanned is never found in smartsheet schedule
        else:

            outcome = False

        return outcome

    def task_start_date(self, issue_key):

        # need to see if epic is assigned and in smartsheet as planned since we do not want to include scoping tasks in
        # Jira that have an epic value that is not planned.  We want to ignore those Jira tasks completely.  These
        # tasks do not represent any work (planned or unplanned).  They are merely scoping tasks and should be ignored.

        # this is a planned task so set start date initially to date as if this came from smartsheet
        task_start_date_time = self.tasks[issue_key]['Start Date']

        # else start date not set so check if created date is set which means this came from jira
        if task_start_date_time is None:
            task_start_date_time = self.tasks[issue_key]['Created Date']

            if task_start_date_time is None:
                self._process_task_problem(issue_key, 'Warning: missing recorded start or created date time.')

        return task_start_date_time

    def calculate_planned_hours(self,
                                start_date,
                                end_date=None,
                                employee_name=None,
                                output_report=False,
                                ignore_fields=[]):

        # Planned information is found in Smartsheet that has task designated start and end dates
        # Jira does not have the notion of start and end dates.  Jira planning is driven by sprints.
        # A Jira task could be a scoping task only that has no work which means it could have an
        # Epic but we ignore those tasks that have epics that are not in the planned smartsheet

        # initialize each employees planned hours
        if not output_report:
            self._initialize_employee_planned()

        row_number = 0
        total_planned_seconds = 0

        # loop thru all tasks loaded
        for issue_key in self.tasks:

            # if the issue key for current task being processed is a planned task dept
            if self._is_planned_task_dept(issue_key):

                # get the assignee
                assignee = self._normalize_name(self.tasks[issue_key]['Assignee'])

                if output_report:
                    if employee_name is not None:
                        if assignee != employee_name:
                            continue

                if self.verbose:
                    print('Issue: %s, %s' % (issue_key, self.tasks[issue_key]))

                # if task in date range of interest and planned in a schedule and not a scoping task
                if self.is_task_in_schedule(issue_key):

                    task_start_date_time = self.task_start_date(issue_key)

                    # if self.tasks[issue_key]['Start Date'] is not None:
                    if self._is_datetime_in_period(task_start_date_time, start_date, end_date):

                        # Only process if assignee in employee info to process
                        if self.is_employee_name_in_employee_info(assignee):

                            # load assignee into planned employees list if not there already
                            if assignee not in self.plannedEmployees:
                                self.plannedEmployees.append(assignee)

                            # get all time estimates
                            original_estimate = self.tasks[issue_key]['Original Estimate']
                            time_spent = self.tasks[issue_key]['Time Spent']

                            if time_spent < original_estimate:
                                time_spent = original_estimate

                            if time_spent > 0:

                                if not output_report:
                                    self.employees[assignee]['planned_work_time'] = \
                                        self.employees[assignee]['planned_work_time'] + time_spent

                            else:
                                self._process_task_problem(issue_key, 'Error: no time recorded by %s' % assignee)
                                if self.verbose:
                                    print('Error: Issue %s has no time recorded' % issue_key)

                            if output_report:
                                print(self._format_task_in_memory_for_report(issue_key, ignore_fields))
                            else:
                                # update the total planned time
                                total_planned_seconds = total_planned_seconds + time_spent

            row_number = row_number + 1

        if not output_report:
            # set total planned hours based on seconds recorded above
            self.total_planned_hours = self.seconds_to_hours(total_planned_seconds)
        return self.total_planned_hours

    def get_planned_employees(self):
        return sorted(self.plannedEmployees, key=lambda x: x.split(" ")[-1])

    def seconds_to_hours(self, time_value):
        return float(time_value) / float(self.seconds_in_hour)

    def _process_task_problem(self, issue_key, problem_description):
        if issue_key in self.tasks:
            self.tasks[issue_key]['Problem'] = problem_description

    def calculate_unplanned_hours(self,
                                  start_date,
                                  end_date=None,
                                  employee_name=None,
                                  output_report=False,
                                  ignore_fields=[]):

        if not output_report:
            # initialize each employees unplanned value
            self._initialize_employee_unplanned()

        row_number = 0
        total_employee_vacation_seconds = 0
        total_unplanned_work_seconds = 0
        total_meeting_seconds = 0

        # process each task that has been loaded
        for issue_key in self.tasks:

            # if the issue key for current task being processed is an unplanned task dept
            if self._is_unplanned_task_dept(issue_key):

                # get task assignee
                assignee = self._normalize_name(self.tasks[issue_key]['Assignee'])
                reporter = self._normalize_name(self.tasks[issue_key]['Reporter'])

                if output_report:
                    if employee_name is not None:
                        if (assignee != employee_name) and (reporter != employee_name):
                            continue

                # if task is not scheduled in a smartsheet project
                if not self.is_task_in_schedule(issue_key):

                    task_start_date_time = self.task_start_date(issue_key)

                    # if current task start date is in the date range of interest
                    if self._is_datetime_in_period(task_start_date_time, start_date, end_date):

                        # determine if this is a vacation task or something else unplanned
                        vacation = self._is_vacation(self.tasks[issue_key]['Issue Type'])
                        unplanned = self.tasks[issue_key]['Unplanned']
                        meeting = self.jira_unplanned_task_departments['meeting'] in issue_key

                        # if this is an unplanned task or vacation entry or meeting
                        if unplanned or vacation or meeting:

                            # load time estimates
                            time_spent = self.tasks[issue_key]['Time Spent']
                            original_estimate = self.tasks[issue_key]['Original Estimate']

                            if time_spent < original_estimate:
                                time_spent = original_estimate

                            # if not vacation
                            if not vacation:

                                if not output_report:

                                    # check if this an employee to be considered
                                    if self.is_employee_name_in_employee_info(assignee):

                                        if not meeting:
                                            # upldate employees unplanned time total
                                            self.employees[assignee]['unplanned_work_time'] = \
                                                self.employees[assignee]['unplanned_work_time'] + time_spent

                                            # add time spent to the total unplanned value for all employees
                                            total_unplanned_work_seconds = total_unplanned_work_seconds + time_spent

                                        else:

                                            # do not add meeting event if it did not occur during workday hours
                                            if self._is_date_time_during_working_hours(
                                                    self.tasks[issue_key]['Start Date']):

                                                self.employees[assignee]['meeting_time'] = \
                                                    self.employees[assignee]['meeting_time'] + time_spent

                                                total_meeting_seconds = total_meeting_seconds + time_spent

                                            else:

                                                self._process_task_problem(
                                                    issue_key,
                                                    'Info: recorded time outside work hours, ignored')

                            # else vacation entry
                            else:

                                # if vacation entry the reporter is the assignee to vacation
                                assignee = reporter

                                if not output_report:

                                    # if the reporter is in employees to be considered
                                    if self.is_employee_name_in_employee_info(assignee):

                                        if self.verbose:
                                            print(original_estimate, assignee)

                                        # set employee vacation time used during period
                                        self.employees[assignee]['vacation_time'] = self.employees[assignee][
                                                                                        'vacation_time'] + time_spent

                                        # add the vacation time to the running total
                                        total_employee_vacation_seconds = total_employee_vacation_seconds + time_spent

                            # if not time recorded than we have a task with an issue and needs fixed
                            if time_spent == 0:
                                problem_task = True
                            # else task has recorded time
                            else:
                                problem_task = False

                            if problem_task:
                                self._process_task_problem(issue_key, 'Error: no time recorded by %s' % assignee)
                                if self.verbose:
                                    print('Error: Issue %s has no time recorded.' % issue_key)

                            if output_report:

                                if employee_name is not None:

                                    if assignee == employee_name:
                                        print(self._format_task_in_memory_for_report(issue_key, ignore_fields))

                            # the assignee is not a engineering team member
                            # ignore the task completely
                            if self.verbose:
                                if not self.is_employee_name_in_employee_info(assignee):
                                    print('Assignee %s is not in engineering dept and therefore activity time ignored' %
                                          assignee)

            row_number = row_number + 1

        if not output_report:
            self.total_meeting_hours = self.seconds_to_hours(total_meeting_seconds)
            self.total_vacation_hours = self.seconds_to_hours(total_employee_vacation_seconds)
            self.total_holiday_hours = self.seconds_to_hours(self.holiday_hours_for_all_employees_in_period(start_date,
                                                                                                            end_date))
            self.total_unplanned_hours = \
                self.seconds_to_hours(total_unplanned_work_seconds) + self.total_vacation_hours + \
                self.total_holiday_hours + self.total_meeting_hours

        return self.total_unplanned_hours

    def _employee_name_from_alias(self, potential_alias):
        if potential_alias in self.employee_aliases.keys():
            employee_name = self.employee_aliases[potential_alias]
        else:
            employee_name = None
        return employee_name

    def _is_employee_name_an_alias(self, employee_name):
        found_employee = False
        if employee_name in self.employee_aliases.keys():
            found_employee = True
        return found_employee

    def is_employee_name_in_employee_info(self, employee_name):
        found_employee = False
        # if the task is in employee names to process
        if employee_name in self.employee_info.keys():
            found_employee = True
        if self._is_employee_name_an_alias(employee_name):
            found_employee = True
        return found_employee

    def _report_column_data_for_task_log_in_memory(self, issue_key, ignore_fields=[]):

        output = []

        if issue_key in self.tasks:

            for log_entry in self.tasks[issue_key]['Work Log']:

                # for each value in the task
                for task_key in self.task_log_output_format:

                    # if the value is not supposed to be ignored
                    if task_key not in ignore_fields:

                        if task_key == 'Assignee':
                            task_value_output = self._normalize_name(log_entry[task_key])

                        # if the task key is a date field format it to month/day/year
                        elif task_key in self.task_date_fields:

                            # if the date field is not None
                            if self.tasks[issue_key][task_key] is not None:
                                task_value_output = log_entry[task_key].strftime('%m/%d/%Y %H:%M')

                            # else output a null
                            else:
                                task_value_output = ''

                        # if task key is a time field then convert all times to hours
                        elif task_key in self.task_time_fields:

                            if self.tasks[issue_key][task_key] is not None:
                                task_value_output = '%1.2f' % self.seconds_to_hours(log_entry[task_key])
                            else:
                                task_value_output = '0.00'

                        # else task output value is same as task field
                        else:
                            task_value_output = log_entry[task_key]

                        output.append(task_value_output)

        return output

    def _report_column_data_for_task_in_memory(self, issue_key, ignore_fields=[]):

        output = []

        if issue_key in self.tasks:

            # for each value in the task
            for task_key in self.task_output_format:

                # if the value is not supposed to be ignored
                if task_key not in ignore_fields:

                    task_value_output = ''

                    if task_key == 'Epic':

                        # if the epic is None
                        if self.tasks[issue_key][task_key] is None:
                            task_value_output = 'None'

                        # else epic is present
                        else:
                            task_value_output = self.tasks[issue_key]['Epic']

                    elif task_key == 'Assignee' or task_key == 'Reporter':
                        task_value_output = self._normalize_name(self.tasks[issue_key][task_key])

                    elif task_key == 'Issue Key':
                        # task output is the issue key
                        task_value_output = issue_key

                    elif task_key == 'Work Log':
                        pass

                    # if the task key is a date field format it to month/day/year
                    elif task_key in self.task_date_fields:

                        # if the date field is not None
                        if self.tasks[issue_key][task_key] is not None:
                            task_value_output = self.tasks[issue_key][task_key].strftime('%m/%d/%Y %H:%M')

                        # else output a null
                        else:
                            task_value_output = ''

                    # if task key is a time field then convert all times to hours
                    elif task_key in self.task_time_fields:

                        if self.tasks[issue_key][task_key] is not None:
                            task_value_output = '%1.2f' % self.seconds_to_hours(self.tasks[issue_key][task_key])
                        else:
                            task_value_output = '0.00'

                    # else task output value is same as task field
                    else:
                        task_value_output = self.tasks[issue_key][task_key]

                    output.append(task_value_output)

        return output

    def workable_hours_for_employee_in_period(self, employee_name, start_date, end_date=None):
        calendar_hours_in_period = self.calendar_hours_for_one_employee_for_period(start_date, end_date)
        holiday_hours_in_period = self.holiday_hours_per_employee(start_date, end_date)
        vacation_hours_in_period = self.seconds_to_hours(self.employees[employee_name]['vacation_time'])
        meeting_hours_in_period = self.seconds_to_hours(self.employees[employee_name]['meeting_time'])
        return calendar_hours_in_period - (holiday_hours_in_period + vacation_hours_in_period + meeting_hours_in_period)

    def _report_column_data_for_task_db(self, task_db_row, ignore_fields=[]):

        output = []

        # for each value in the task
        for task_key in self.task_output_format:

            # if the value is not supposed to be ignored
            if task_key not in ignore_fields:

                task_value_output = ''

                if task_key == 'Epic':

                    # if the epic is None
                    if task_db_row['Epic'] is None:
                        task_value_output = 'None'

                    # else epic is present
                    else:
                        task_value_output = task_db_row['Epic']

                elif task_key == 'Assignee' or task_key == 'Reporter':
                    task_value_output = self._normalize_name(task_db_row[task_key])

                elif task_key == 'Issue Key':
                    # task output is the issue key
                    task_value_output = task_db_row['Issue Key']

                elif task_key == 'Work Log':
                    pass

                # if the task key is a date field format it to month/day/year
                elif task_key in self.task_date_fields:

                    # if the date field is not None
                    if task_db_row[task_key] is not None:
                        task_value_output = task_db_row[task_key].strftime('%m/%d/%Y %H:%M')

                    # else output a null
                    else:
                        task_value_output = ''

                # if task key is a time field then convert all times to hours
                elif task_key in self.task_time_fields:

                    if task_db_row[task_key] is not None:
                        task_value_output = '%1.2f' % self.seconds_to_hours(task_db_row[task_key])
                    else:
                        task_value_output = '0.00'

                # else task output value is same as task field
                else:
                    task_value_output = task_db_row[task_key]

                output.append(task_value_output)

        return output

    def _format_task_fetched_from_db_for_report(self, database_row, ignore_fields=[]):

        output = self._report_column_data_for_task_db(task_db_row=database_row, ignore_fields=ignore_fields)

        return _csv_row_to_string(output)

    def _format_task_in_memory_for_report(self, issue_key, ignore_fields=[]):

        output = self._report_column_data_for_task_in_memory(issue_key, ignore_fields)

        return _csv_row_to_string(output)

    def _format_task_log_in_memory_for_report(self, issue_key, ignore_fields=[]):

        output = self._report_column_data_for_task_log_in_memory(issue_key, ignore_fields)

        return _csv_row_to_string(output)

    def _set_date_range(self, start_date, end_date):

        new_start_date = self._get_next_weekday_from_date(start_date, self.business_hours_date_format)
        if start_date != new_start_date:
            print('Specified start date %s is not on a weekday, advancing start date to %s' % (
                start_date, new_start_date))
            start_date = new_start_date

        if end_date is not None:
            new_end_date = self._get_next_weekday_from_date(end_date, self.business_hours_date_format)
            if end_date != new_end_date:
                print('Specified end date %s is not on a weekday, advancing end date to %s' % (end_date, new_end_date))
                end_date = new_end_date
        else:
            end_date_time = datetime.now()
            end_date = end_date_time.strftime('%m/%d/%Y %H:%M')

        return start_date, end_date

    def generate_report(self, report_type, start_date, end_date=None, output='display', filename=None):

        self._set_date_range(start_date, end_date)

        ignore_fields = []

        if output == 'display':

            if report_type == 'all planned' or report_type == 'all unplanned':

                if report_type == 'all unplanned':
                    ignore_fields = ['Description', 'Progress', 'Unplanned', 'Reporter',
                                     'End Date', 'Start Date', 'Resolution', 'Issue Type', 'Epic', 'Work Log']
                    self.calculate_unplanned_hours(start_date=start_date, end_date=end_date)

                elif report_type == 'all planned':
                    ignore_fields = ['Description', 'Progress', 'Unplanned', 'Reporter',
                                     'End Date', 'Start Date', 'Resolution', 'Issue Type', 'Work Log']
                    self.calculate_planned_hours(start_date=start_date, end_date=end_date)

                print()
                print('===================================')
                print('%s Report for Period' % (string.capwords(report_type)))
                print('  Start Date: %s' % start_date)
                print('  End Date: %s' % end_date)
                for current_employee in self.employee_names:
                    print()
                    print('Employee Name: %s' % current_employee)
                    if report_type == 'all unplanned':
                        print('Meeting Hours: %1.2f' % self.seconds_to_hours(
                            self.employees[current_employee]['meeting_time']))
                        print('Vacation Hours: %1.2f' % self.seconds_to_hours(
                            self.employees[current_employee]['vacation_time']))
                    print('Tasks:')
                    header = []
                    for task_key in self.task_output_format:
                        if task_key not in ignore_fields:
                            header.append(task_key)

                    print(_csv_row_to_string(header))

                    ignore_fields = ignore_fields + ['Assignee']
                    # for each issue key in tasks

                    if report_type == 'all planned':

                        self.calculate_planned_hours(start_date=start_date,
                                                     end_date=end_date,
                                                     employee_name=current_employee,
                                                     output_report=True,
                                                     ignore_fields=ignore_fields)

                    elif report_type == 'all unplanned':

                        self.calculate_unplanned_hours(start_date=start_date,
                                                       end_date=end_date,
                                                       employee_name=current_employee,
                                                       output_report=True,
                                                       ignore_fields=ignore_fields)

                print('===================================')

            elif report_type == 'task errors':

                self.calculate_planned_hours(start_date=start_date, end_date=end_date)
                self.calculate_unplanned_hours(start_date=start_date, end_date=end_date)

                ignore_fields = ['Description', 'Progress', 'Unplanned', 'Reporter',
                                 'End Date', 'Start Date', 'Resolution', 'Issue Type', 'Epic', 'Work Log']

                print()
                print('===================================')
                print('%s Report for Period' % (string.capwords(report_type)))
                print('  Start Date: %s' % start_date)
                print('  End Date: %s' % end_date)
                for current_employee in self.employee_names:
                    print()
                    print('Employee Name: %s' % current_employee)
                    print('Errors:')
                    header = []
                    for task_key in self.task_output_format:
                        if task_key not in ignore_fields:
                            header.append(task_key)

                    print(_csv_row_to_string(header))

                    ignore_fields = ignore_fields + ['Assignee']

                    # for each issue key in tasks
                    for issue_key in self.tasks:

                        task_ignore_fields = ignore_fields

                        # if an error description is in task then
                        if self.tasks[issue_key]['Problem'] is not None:

                            # if the task is in employee names to process
                            if self._normalize_name(self.tasks[issue_key]['Assignee']) == current_employee:

                                date_time = self.tasks[issue_key]['Created Date']

                                if not self.tasks[issue_key]['Unplanned']:
                                    date_time = self.tasks[issue_key]['Start Date']

                                if self._is_datetime_in_period(date_time, start_date, end_date):
                                    # print the formatted task line to output device
                                    print(self._format_task_in_memory_for_report(issue_key, task_ignore_fields))

                print('===================================')

            elif report_type == 'work logged':

                print()
                print('===================================')
                print('%s Report for Period' % (string.capwords(report_type)))
                print('  Start Date: %s' % start_date)
                print('  End Date: %s' % end_date)

                print(_csv_row_to_string(['Assignee', 'Logged Hours', 'Workable Hours']))

                for current_employee in self.task_work_logged:

                    if self.is_employee_name_in_employee_info(current_employee):
                        employee_workable_hours = '%1.2f' % self.workable_hours_for_employee_in_period(current_employee,
                                                                                                       start_date,
                                                                                                       end_date)

                        logged_work = \
                            '%1.2f' % self.seconds_to_hours(
                                self.task_work_logged[current_employee]['total_logged_work'])

                        print(_csv_row_to_string([current_employee, logged_work, employee_workable_hours]))

            elif report_type == 'dept breakdown':

                self.calculate_planned_hours(start_date=start_date, end_date=end_date)
                self.calculate_unplanned_hours(start_date=start_date, end_date=end_date)

                dept_breakdown = {}

                # process all depts planned or unplanned
                for dept in self.jira_unplanned_task_departments:

                    dept_breakdown[dept] = {'unplanned_work_time': 0, 'planned_work_time': 0, 'vacation_time': 0,
                                            'meeting_time': 0}

                    if dept != 'meeting':

                        for current_employee in self.employee_names:

                            if dept in self.employee_info[current_employee]['dept']:
                                dept_breakdown[dept]['unplanned_work_time'] = dept_breakdown[dept][
                                                                                  'unplanned_work_time'] + \
                                                                              self.employees[current_employee][
                                                                                  'unplanned_work_time']

                                dept_breakdown[dept]['planned_work_time'] = \
                                    dept_breakdown[dept]['planned_work_time'] + \
                                    self.employees[current_employee]['planned_work_time']

                                dept_breakdown[dept]['vacation_time'] = \
                                    dept_breakdown[dept]['vacation_time'] + \
                                    self.employees[current_employee]['vacation_time']

                    # else meeting data to be computed
                    else:
                        for current_employee in self.employee_names:
                            dept_breakdown[dept]['meeting_time'] = dept_breakdown[dept]['meeting_time'] + \
                                                                   self.employees[current_employee]['meeting_time']

                print()
                for dept in self.jira_unplanned_task_departments.keys():
                    print('===================================')
                    print('Department Breakdown for Period')
                    print('Start Date: %s' % start_date)
                    print('End Date: %s' % end_date)
                    print('Dept: %s' % string.capwords(dept))
                    print(
                        'Unplanned Hours: %1.2f' % self.seconds_to_hours(dept_breakdown[dept]['unplanned_work_time']))
                    print('Planned Hours: %1.2f' % self.seconds_to_hours(dept_breakdown[dept]['planned_work_time']))
                    print('Vacation Hours: %1.2f' % self.seconds_to_hours(dept_breakdown[dept]['vacation_time']))
                    print('Meeting Hours: %1.2f' % self.seconds_to_hours(dept_breakdown[dept]['meeting_time']))
                    total_all_unplanned_hours = self.seconds_to_hours(dept_breakdown[dept]['unplanned_work_time'] +
                                                                      dept_breakdown[dept]['vacation_time'] +
                                                                      dept_breakdown[dept]['meeting_time'])

                    try:
                        print('Percentage Unplanned to Planned: %1.2f%%' %
                              (total_all_unplanned_hours /
                               self.seconds_to_hours(dept_breakdown[dept]['planned_work_time'])) * 100.0)
                    except:
                        pass

                    try:
                        print('Percentage Unplanned to (Planned + Unplanned): %1.2f%%' %
                              (total_all_unplanned_hours /
                               (self.seconds_to_hours(dept_breakdown[dept]['planned_work_time']) +
                                total_all_unplanned_hours)) * 100.0)
                    except:
                        pass

                    print('Tasks:')
                    ignore_fields = ['Description', 'Progress', 'Unplanned', 'Reporter',
                                     'End Date', 'Start Date', 'Resolution', 'Issue Type', 'Epic', 'Work Log']

                    header = []
                    for task_key in self.task_output_format:
                        if task_key not in ignore_fields:
                            header.append(task_key)
                    print(_csv_row_to_string(header))

                    for issue_key in self.tasks:

                        if self.jira_unplanned_task_departments[dept] in issue_key:

                            if self.is_employee_name_in_employee_info(self.tasks[issue_key]['Assignee']):
                                print(self._format_task_in_memory_for_report(issue_key, ignore_fields))

            elif report_type == 'employee hours summary':

                self.calculate_planned_hours(start_date=start_date, end_date=end_date)
                self.calculate_unplanned_hours(start_date=start_date, end_date=end_date)

                print()
                print('===================================')
                print('Employee Hours Summary for Period')
                print('Start Date: %s' % start_date)
                print('End Date: %s' % end_date)
                print('Name, Unplanned, Planned, Vacation')
                for current_employee in self.employee_names:
                    print('%s,%1.2f,%1.2f,%1.2f' % (current_employee,
                                                    self.seconds_to_hours(
                                                        self.employees[current_employee]['unplanned_work_time']),
                                                    self.seconds_to_hours(
                                                        self.employees[current_employee]['planned_work_time']),
                                                    self.seconds_to_hours(
                                                        self.employees[current_employee]['vacation_time'])))
                print('===================================')

            elif report_type == 'planned employees':

                self.calculate_planned_hours(start_date=start_date, end_date=end_date)
                self.calculate_unplanned_hours(start_date=start_date, end_date=end_date)

                print()
                print('===================================')
                print('Employee(s) who have worked on scheduled projects in Period:')
                print('Start Date: %s' % start_date)
                print('End Date: %s' % end_date)
                for current_employee in self.get_planned_employees():
                    print(current_employee)
                print('===================================')

            elif report_type == 'work statistics':

                total_possible_work_hours_in_year = self.total_workable_hours(start_date=None,
                                                                              end_date=None)
                total_possible_work_hours_in_period = self.total_workable_hours(start_date=start_date,
                                                                                end_date=end_date)
                total_holiday_hours_in_year = \
                    self.holiday_hours_for_all_employees_in_period(start_date=start_date,
                                                                   end_date='12/31/2018 23:59')

                total_unplanned_hours = self.calculate_unplanned_hours(start_date=start_date,
                                                                       end_date=end_date)

                total_planned_hours = self.calculate_planned_hours(start_date=start_date,
                                                                   end_date=end_date)

                total_planned_and_unplanned_hours = self.total_planned_and_unplanned_hours(start_date=start_date,
                                                                                           end_date=end_date)

                total_percentage_work_unplanned = self.percentage_unplanned_to_planned(start_date=start_date,
                                                                                       end_date=end_date)

                total_percentage_work_unplanned_and_planned = (float(total_unplanned_hours) /
                                                               float(total_planned_and_unplanned_hours)) * 100.0

                total_number_employees = len(self.employee_names)
                print()
                print('===================================')
                print('Task Statistics')
                print('Total Number of Employees: %d' % total_number_employees)
                print('All statistics below are for all employees for a year')
                print('Maximum Work Hours (Excludes Weekends): %d' %
                      self.calendar_hours_for_all_employees_for_current_year())

                print('Maximum Vacation Hours: %d' % self.maximum_vacation_hours_that_can_be_recorded_for_employees())

                print('Maximum Holiday Hours: %d' % total_holiday_hours_in_year)

                print('Maximum Work Hours (Calendar Hours - (Vacation and Holidays)) for Year: %d' %
                      total_possible_work_hours_in_year)
                print()
                print('All statistics below are for all Employees during specified period')
                print('Start Date: %s' % start_date)
                print('End Date: %s' % end_date)

                print('Total Calendar Hours - (Reported Vacation and Holidays): %d' %
                      total_possible_work_hours_in_period)

                print('Total Unplanned Hours (Includes Vacations and Holidays): %d ' % total_unplanned_hours)

                print('Total Planned Hours (Scheduled Projects): %d' % total_planned_hours)

                print('Total Planned + Unplanned Worked Hours = %d' % total_planned_and_unplanned_hours)

                print(
                    'Total Percentage of all unplanned in relation to max possible hours that could be worked: %1.2f%%'
                    % ((float(total_unplanned_hours) / float(total_possible_work_hours_in_period)) * 100.0))

                print(
                    'Total percentage of all reported work that is unplanned (unplanned/(unplanned+planned)) = %1.2f%%'
                    % total_percentage_work_unplanned_and_planned)

                print('Total percentage of all unplanned in relation to all planned (unplanned/planned) = %1.2f%%' %
                      total_percentage_work_unplanned)

                print('Unplanned Percentage Range: (%d%% - %d%%)' %
                      (total_percentage_work_unplanned_and_planned, total_percentage_work_unplanned))

                print('Unplanned Percentage Range Midpoint: %d%%' %
                      ((total_percentage_work_unplanned_and_planned + total_percentage_work_unplanned) / 2))

            elif report_type == 'input file statistics':
                print()
                print('===================================')
                print('File Export Statistics')
                print("Today's Date is %s" % (_datetime_to_date_string(datetime.now())))
                # print('Jira wildcard files "%s" exported on %s' % (self.jira_file_wildcard, self.lastInputFileDate)
                # print('Smartsheet file %s exported on %s' % (self.smartsheet_filename, self.lastSmartsheetExportDate)
                # print('Last time Jira and Smartsheet export files were analyzed %s' %
                # (self.lastTimeJiraInputFileModifiedDate)

                # if self.dateStringToDateTime(self.lastTimeJiraInputFileModifiedDate, self.business_hours_date_format)
                #  < \
                #        self.dateStringToDateTime(self.lastSmartsheetExportDate, self.business_hours_date_format):
                #    print('Smartsheet Export File needs processed, re-run task analysis'
                # if self.dateStringToDateTime
                #  < \
                #        self.dateStringToDateTime(self.lastInputFileDate, self.business_hours_date_format):
                #    print('Jira Export File(s) need processed, re-run task analysis'

        elif output == 'file':

            if report_type == 'all tasks csv dump' or report_type == 'all tasks csv dump in period':

                include_in_report = True

                task_file = open(file=filename, mode='w', errors='ignore')

                task_writer = csv.writer(task_file,
                                         delimiter=',',
                                         quotechar='"',
                                         quoting=csv.QUOTE_MINIMAL)

                columns = []
                for column_header in self.task_output_format:
                    if column_header != 'Issue Key' and column_header != 'Work Log':
                        columns.append(column_header)
                task_writer.writerow(['Issue Key'] + columns)

                for issue_key in self.tasks:

                    if report_type == 'all tasks csv dump in period':

                        task_start_date_time = self.task_start_date(issue_key)

                        include_in_report = False

                        if self._is_datetime_in_period(task_start_date_time, start_date, end_date):
                            include_in_report = True

                    if include_in_report:
                        output = self._report_column_data_for_task_in_memory(
                            issue_key,
                            ignore_fields=['Issue Key'])
                        task_writer.writerow([issue_key] + output)

                task_file.close()

    def datetime_for_first_day_in_current_year(self):
        date = _date_string_for_first_day_in_current_year()
        date_time = _date_string_to_datetime(date, self.business_hours_date_format)
        return date_time

    def datetime_for_last_day_in_current_year(self):
        date = _date_string_for_last_day_in_current_year()
        date_time = _date_string_to_datetime(date, self.business_hours_date_format)
        return date_time

    def current_year_datetime_range(self):
        start_date_time = self.datetime_for_first_day_in_current_year()
        end_date_time = self.datetime_for_last_day_in_current_year()
        return start_date_time, end_date_time

    def calendar_hours_for_one_employee_for_current_year(self):

        start_date_time, end_date_time = self.current_year_datetime_range()

        business_days = BusinessHours.BusinessHours(datetime1=start_date_time,
                                                    datetime2=end_date_time,
                                                    worktiming=self.workDayTime,
                                                    weekends=self.weekends)

        single_employee_work_hours_in_period = business_days.gethours()
        return single_employee_work_hours_in_period

    def calendar_hours_for_all_employees_for_current_year(self):

        return self.calendar_hours_for_one_employee_for_current_year() * len(self.employee_names)

    def maximum_vacation_hours_that_can_be_recorded_for_employees(self):
        total_vacation_hours = 0
        for current_employee in self.employee_names:
            total_vacation_hours = total_vacation_hours + (self.employee_info[current_employee]['vacation_days'] * 8)
        return total_vacation_hours

    def calendar_hours_for_one_employee_for_period(self, start_date, end_date=None):

        start_date_time = _date_string_to_datetime(start_date, self.business_hours_date_format)

        if not end_date:
            end_date_time = datetime.now()
        else:
            end_date_time = _date_string_to_datetime(end_date, self.business_hours_date_format)

        business_days = BusinessHours.BusinessHours(datetime1=start_date_time,
                                                    datetime2=end_date_time,
                                                    worktiming=self.workDayTime,
                                                    weekends=self.weekends)
        return business_days.gethours()

    def calendar_hours_for_all_employees_for_period(self, start_date, end_date=None):

        return len(self.employee_names) * self.calendar_hours_for_one_employee_for_period(start_date, end_date)

    def total_workable_hours(self, start_date, end_date=None):

        # no start date means we are calculating from start of a year
        if start_date is None:
            # work hours need calculate for entire year
            total_vacation_hours = self.maximum_vacation_hours_that_can_be_recorded_for_employees()
            start_date = _date_string_for_first_day_in_current_year()

            if end_date is None:
                # set end date to end of year
                end_date = _date_string_for_last_day_in_current_year()

        else:
            self.calculate_unplanned_hours(start_date, end_date)
            total_vacation_hours = self.total_vacation_hours

        total_holiday_hours_in_period = self.holiday_hours_for_all_employees_in_period(start_date, end_date)

        if start_date is None:
            calendar_hours = self.calendar_hours_for_all_employees_for_current_year()
        else:
            calendar_hours = self.calendar_hours_for_all_employees_for_period(start_date, end_date)

        total_workable_hours = calendar_hours - (total_holiday_hours_in_period + total_vacation_hours)
        return total_workable_hours

    def total_planned_and_unplanned_hours(self, start_date, end_date=None):
        total_planned_hours = self.calculate_planned_hours(start_date, end_date)
        total_unplanned_hours = self.calculate_unplanned_hours(start_date, end_date)
        return total_planned_hours + total_unplanned_hours

    def percentage_unplanned_to_planned(self, start_date, end_date=None):
        total_planned_hours = self.calculate_planned_hours(start_date, end_date)
        total_unplanned_hours = self.calculate_unplanned_hours(start_date, end_date)
        if self.verbose:
            print(total_unplanned_hours)
            print(total_planned_hours)
        # total_percentage_unplanned = float(self.total_unplanned_hours) / (float(self.work_hours_in_period *
        # float(len(self.employees)))) * 100.0
        total_percentage_unplanned = (float(total_unplanned_hours) / float(total_planned_hours)) * 100.0
        # total_percentage_unplanned = (float(self.total_unplanned_hours) / float(self.actualWorkHours())) * 100.0
        return total_percentage_unplanned
