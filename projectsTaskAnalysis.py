import taskAnalysis

start_date = '8/1/2017 00:00'
end_date = None

# start_date = '2/10/2018 00:00'

jira_cloud_url = 'atlassian_url'
jira_login_username = 'email'
jira_login_password = 'password'

jira_unplanned_activity_field_name = 'customfield_10049'
epic_field_name = 'customfield_10008'

planned_task_depts = {'software': 'SOF', 'electrical': 'EL', 'mechanical': 'MEC', 'test': 'TEST'}
unplanned_task_depts = {'sustaining': 'SUS', 'sourcing': 'SUP'}

employee_info = {
    'Firstname Lastname1': {'group': 'engineering', 'dept': 'electrical', 'vacation_days': 22, 'aliases': None},
    'Firstname Lastname2': {'group': 'engineering', 'dept': 'software', 'vacation_days': 17, 'aliases': None},
    'Firstname Lastname3': {'group': 'engineering', 'dept': 'test', 'vacation_days': 27, 'aliases': ['Pevoala']},
    'Firstname Lastname4': {'group': 'engineering', 'dept': 'mechanical', 'vacation_days': 27, 'aliases': None},
    'Firstname Lastname5': {'group': 'engineering', 'dept': 'sustaining', 'vacation_days': 17, 'aliases': None},
    'Firstname Lastname6': {'group': 'engineering', 'dept': 'mechanical', 'vacation_days': 27, 'aliases': None},
    'Firstname Lastname7': {'group': 'engineering', 'dept': 'mechanical', 'vacation_days': 17, 'aliases': None},
    'Firstname Lastname8': {'group': 'engineering', 'dept': 'electrical', 'vacation_days': 27, 'aliases': None},
    'Firstname Lastname9': {'group': 'engineering', 'dept': 'software', 'vacation_days': 22, 'aliases': None},
    'Firstname Lastname10': {'group': 'engineering', 'dept': 'software', 'vacation_days': 22,
                      'aliases': ['Juan Guzman', 'Juan Carlos Guzman']},
    'Firstname Lastname11': {'group': 'engineering', 'dept': 'software', 'vacation_days': 22, 'aliases': None},
    'Firstname Lastname12': {'group': 'engineering', 'dept': 'test', 'vacation_days': 22, 'aliases': ['Highlje']},
    'Firstname Lastname13': {'group': 'engineering', 'dept': 'software', 'vacation_days': 17, 'aliases': None},
    'Firstname Lastname14': {'group': 'engineering', 'dept': 'test', 'vacation_days': 22, 'aliases': None},
    'Firstname Lastname15': {'group': 'engineering', 'dept': 'test', 'vacation_days': 17, 'aliases': None},
    'Firstname Lastname16': {'group': 'engineering', 'dept': 'software', 'vacation_days': 0, 'aliases': None}
}

scheduled_project_list = {'Project Name 1': {'id': '238975370745455'},
                          'Project Name 2': {'id': '730634645454545'}}

smartsheet_admin_token = 'dcf34bj2p6awmkcy2f02pga46l'

mailserver_domain_names = ['mail_server_url1', 'mail_server_url2']

projectTasking = taskAnalysis.Processor(employee_info=employee_info,
                                       calendar_file_wildcard='*_calendar.csv',
                                       company_name='Enter_Company_Name',
                                       jira_cloud_url=jira_cloud_url,
                                       jira_login_username=jira_login_username,
                                       jira_login_password=jira_login_password,
                                       jira_planned_task_departments=planned_task_depts,
                                       jira_unplanned_task_departments=unplanned_task_depts,
                                       smartsheet_projects=scheduled_project_list,
                                       update_smartsheet_progress=False,
                                       jira_epic_field_name=epic_field_name,
                                       jira_unplanned_activity_field_name=jira_unplanned_activity_field_name,
                                       database_filename='tasks.sqlite',
                                       smartsheet_access_token=smartsheet_admin_token,
                                       start_date=start_date,
                                       end_date=end_date,
                                       holidays_file='holidays.dat',
                                       mail_server_domain_names=mailserver_domain_names,
                                       verbose=False)

projectTasking.generate_report(report_type='all tasks csv dump',
                              start_date=start_date,
                              end_date=end_date,
                              output='file',
                              filename='tasks.csv')

projectTasking.generate_report(report_type='all tasks csv dump in period',
                              start_date=start_date,
                              end_date=end_date,
                              output='file',
                              filename='tasks_in_period.csv')

projectTasking.generate_report(report_type='all unplanned', start_date=start_date, end_date=end_date)
projectTasking.generate_report(report_type='all planned', start_date=start_date, end_date=end_date)
# projectTasking.generate_report(report_type='employee hours summary', start_date=start_date, end_date=end_date)
projectTasking.generate_report(report_type='planned employees', start_date=start_date, end_date=end_date)
projectTasking.generate_report(report_type='work logged', start_date=start_date, end_date=end_date)
projectTasking.generate_report(report_type='dept breakdown', start_date=start_date, end_date=end_date)
# projectTasking.generate_report(report_type='input file statistics', start_date=start_date, end_date=end_date)
projectTasking.generate_report(report_type='task errors', start_date=start_date, end_date=end_date)
projectTasking.generate_report(report_type='work statistics', start_date=start_date, end_date=end_date)
