# taskAnalysis
Jira and Smartsheet Task Analysis

If you use Jira and Smartsheet and you are looking to analyze your engineering team tasking and projects with regard to Agile and its two week Sprint model as well as a more long term view of a complete waterfall based project with tasking that has a start and end date this Python based program might be for you.  

Inevitably all engineering teams need to be able to bucket work into small tasks that can be monitored in smaller increments.  Agile is great for this and keeps your engineering team focused. Agile however does not have a notion of task start and end dates. The Focus is on bucketing two weeks of work for your team members into a Sprint. Agile is not concerned with a start and end date on a task.  This can create problems in determining if your team will meet product release dates.  Project Managers are looking for engineering to supply schedules that include start and end dates for projects so they can resource level the engineering team across multiple projects and plan for product release dates. 

This program tracks planned scheduled tasks and unplanned tasks that appear out of nowhere that must be worked into planned activities. Product teams developing next generation products in many cases must support existing product already released. Smaller engineering teams are the norm and efficiency and planning is critical so these teams deliver.  Unplanned activities like Software Bugs, Warranty, and End of Life issues impacting a product must be addressed as they occur.  These activities can affect planned project schedules and need to be monitored. 

The program captures input from Jira and Smartsheet to compile a list of tasks that are merged to create a set of files that can be parsed and reported on which address the needs of Engineering and Project Managers. This program uses a REST API to contact the Jira and Smartsheet cloud service you are using and retrieve all tasks in Jira and Smartsheet schedules. Once the tasks are retrieved they are merged so various reports can ge run to analyze the tasks. The program supports a project tracking model where Jira is used to capture all tasking activities performed by an engineering team.  These tasks are unplanned and planned activities that are managed thru Agile SCRUM and sprints.  Jira has the Agile extension to enable Sprint board creation and manipulation.  This allows these tasks to have Epic and tasks assigned to Epics.  These Epics would then thru the Smartsheet Connector product synchronize with Smartsheet where traditional waterfall projects with start and end dates are managed. These project schedules are used by management to resource level the engineering team across multiple projects and to understand when projects will start and end.  Agile focuses on two week sprints and accomplishing a set of tasks within that Sprint.  It does not typically look at the entire work effort of a project or provide the ability to resource level your engineering team members to get a picture of when projects can start and end.

This program provides methods to analyze task activities impact on your team.  These activities include:
- Vacation (Table Argument of Employees and Vacation Time)
- Holidays (Holiday File)
- Meetings (Outlook Calendar Export)
- Unscheduled work tasks that must be complete inside planned scheduled work (Jira)
- Planned scheduled work (Smartsheet)

Meetings can positively or negatively impact team performance.  Microsoft Outlook Calendar exports of your team members meetings can be used as input to this program for analysis.  The amount of time your team is spending in meetings as well as the focus of the meetings that are being held can be analyzed.  Team member vacation plans are not all provided at one time so effective planned schedules can be defined.

The program provides a baseline set of reporting to perform the following:
- Percentage of unplanned (Jira) to planned (Smartsheet) activities

