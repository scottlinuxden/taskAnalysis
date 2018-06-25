# coding=utf-8
"""
Smartsheet schedule module to read specified sheet rows and
optionally update progress
"""
__author__ = 'Scott Davis'

import smartsheet
import logging
from dateutil.parser import parse
import sys


def time_to_seconds(time_value):
    seconds_in_minute = 60
    seconds_in_hour = 60 * seconds_in_minute
    seconds_in_day = 8 * seconds_in_hour
    seconds_in_week = 5 * seconds_in_day

    if time_value.strip() != '':
        if time_value[-1:] == 's':
            time_in_seconds = int(time_value)
        elif time_value[-1:] == 'h':
            time_in_seconds = int(time_value[:-1]) * seconds_in_hour
        elif time_value[-1:] == 'd':
            time_in_seconds = int(time_value[:-1]) * seconds_in_day
        elif time_value[-1:] == 'm':
            time_in_seconds = int(time_value[:-1]) * seconds_in_minute
        elif time_value[-1:] == 'w':
            time_in_seconds = int(time_value[:-1]) * seconds_in_week
        else:
            time_in_seconds = int(time_value.strip())
    else:
        time_in_seconds = 0

    return time_in_seconds


def date_string_to_date_time(date):
    date_time = None
    if date is not None:
        if date.strip() != '':
            date_time = parse(date.strip())
    return date_time


class SmartsheetProcessor:

    def __init__(self,
                 company_name,
                 access_token,
                 smartsheet_projects,
                 normalize_assignee,
                 update_sheet_progress=False):

        self.company_name = company_name

        self.smartsheet_projects = smartsheet_projects
        self.update_sheet_progress = update_sheet_progress
        self.normalize_assignee = normalize_assignee

        self.sheet_ids = []
        for project in self.smartsheet_projects:
            self.sheet_ids.append(self.smartsheet_projects[project]['id'])

        self.smartsheet_instance = smartsheet.Smartsheet(access_token)

        self.smartsheet_instance.errors_as_exceptions(True)

        logging.basicConfig(filename='rwsheet.log', level=logging.INFO)

        self.tasks = {}

        self._process_project_tasks()

    def _process_project_tasks(self):

        print()
        print()
        print('Smartsheet Project Schedule Retrieval for site: %s' % self.company_name)

        page_size = 100
        number_of_tasks_in_all_projects = 0

        for current_project in self.smartsheet_projects:

            print()
            print('Processing scheduled task(s) in Smartsheet: %s' % current_project)

            page_number = 0
            number_of_tasks_in_current_project = 0
            current_sheet_id = self.smartsheet_projects[current_project]['id']

            while True:

                page_number += 1
                sys.stdout.write("\rFetching %d schedule rows at a time starting on page %d" % (page_size, page_number))
                sys.stdout.flush()

                # Load next 100 items from sheet
                sheet = self.smartsheet_instance.Sheets.get_sheet(current_sheet_id,
                                                                  page_size=page_size,
                                                                  page=page_number)

                if page_number == 1:
                    self.column_map = {}
                    # Build column map for later reference - translates column names to column id
                    for column in sheet.columns:
                        self.column_map[column.title] = column.id

                self._process_tasks(sheet, current_sheet_id)

                number_of_tasks_in_current_project += len(sheet.rows)

                if len(sheet.rows) != page_size:
                    break

            number_of_tasks_in_all_projects += number_of_tasks_in_current_project
            print('\nTotal number of scheduled task(s) in project %s processed: %d' %
                  (current_project, number_of_tasks_in_current_project))

        print()
        print('Total number of schedule task(s) processed for all specified Smartsheet(s): %d' %
              number_of_tasks_in_all_projects)

    def _update_row_progress(self, row, issue_key):

        # progress_cell = self.get_cell_by_column_name(source_row, "Progress")
        # progress_value = progress_cell.display_value

        try:
            remaining_estimate = float(self.tasks[issue_key]['Remaining Estimate'])
        except:
            remaining_estimate = 0.0

        try:
            time_spent = float(self.tasks[issue_key]['Time Spent'])
        except:
            time_spent = 0.0

        if int(remaining_estimate) == 0:
            progress_value = 1
        else:
            progress_value = float(time_spent) / float(remaining_estimate + time_spent)

        # Build new cell value
        new_cell = self.smartsheet_instance.models.Cell()
        new_cell.column_id = self.column_map["Progress"]
        new_cell.strict = True
        new_cell.value = progress_value

        # Build the row to update
        new_row = self.smartsheet_instance.models.Row()
        new_row.id = row.id
        new_row.cells.append(new_cell)

        return new_row

    def get_cell_by_column_name(self, row, column_name, is_date=False):
        column_id = self.column_map[column_name]
        cell = row.get_column(column_id)

        if is_date:
            data_value = date_string_to_date_time(cell.value)
        else:
            data_value = cell.display_value
        return data_value

    def scheduled_tasks(self):
        return self.tasks

    def scheduled_task_issues(self):
        return self.tasks.keys()

    def _process_tasks(self, sheet, sheet_id):

        updated_progress_rows = []

        for row in sheet.rows:
            issue_key = self.get_cell_by_column_name(row, 'Issue Key')

            # task must have an issue key or it should not be included
            if issue_key is not None:

                # if no assignee this is a roll-up task so ignore
                if self.get_cell_by_column_name(row, 'Assignee') is not None:

                    self.tasks[issue_key] = {}
                    self.tasks[issue_key]['Assignee'] = \
                        self.normalize_assignee(self.get_cell_by_column_name(row, 'Assignee'))
                    self.tasks[issue_key]['Summary'] = self.get_cell_by_column_name(row, 'Summary')
                    self.tasks[issue_key]['Start Date'] = \
                        self.get_cell_by_column_name(row, 'Start Date', is_date=True)
                    self.tasks[issue_key]['End Date'] = \
                        self.get_cell_by_column_name(row, 'End Date', is_date=True)
                    self.tasks[issue_key]['Remaining Estimate'] = \
                        self.get_cell_by_column_name(row, 'Remaining Time Estimated')
                    self.tasks[issue_key]['Original Estimate'] = \
                        self.get_cell_by_column_name(row, 'Original Time Estimated')
                    self.tasks[issue_key]['Time Spent'] = self.get_cell_by_column_name(row, 'Time Spent')
                    self.tasks[issue_key]['Problem'] = None
                    # print self.tasks[issue_key]

                    if self.update_sheet_progress:
                        progress_row = self._update_row_progress(row, issue_key)
                        updated_progress_rows.append(progress_row)

        if len(updated_progress_rows) > 0:
            return self.smartsheet_instance.Sheets.update_rows(sheet_id, updated_progress_rows)
