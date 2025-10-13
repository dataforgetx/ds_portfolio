## About The Project

In response to increased complaints of use of force (UoF) from probationed youths and their families, Los Angeles County Probation Department (LACPD) decided to implement an early intervention system (EIS), which was intended to flag staff who might be demonstrating consistent patterns of heightened UoF and determine if interventions (i.e., staff training programs) might be warranted.

EIS is composed of:

1. Source Data Systems, including Oracle, SQL Server databases, and flat files (Excel)
2. Algorithms to flag staff (written in R)
3. Interactive visualization to display flagged staff

```mermaid
graph LR
  A[Source Data] --> B[EIS Algorithms]
  B --> C[Power BI Dashboard]
```

At the beginning of a week, supervisory staff would check the dashboard to get a list of flagged staff for their assigned units. Then they can further drill down in the dashboard to see UoF incidents for the staff in question over the past week or month. Based on the results, they can decide if interventions are warranted.

### Algorithms

##### Youth-based Intervention Score

1. Hold constant the youth receiving intervention and compares staff who intervened with that same youth
2. Pull all staff incident reports (PIR, SIR) involving a specific youth in the past months.
   a. We are talking about many incidents per youth.
   b. Select the highest level of intervention per incident per staff
3. Calculate for each involved staff member what their level of intervention was relative to the average of all other staff who intervened with that same youth.
   a. Each staff’s level meaning the average across all incidents with that youth
4. Subtract the average of other staff’s intervention levels with that specific youth from that staff member’s intervention level.
5. Repeat the process for all youth who received interventions from more than 1 staff member in the past two months.
6. Take the average across all youth for that staff member

##### Incident-based Intervention Score

1. Hold constant the incident in question and compares staff who intervened during that same incident.
2. If there are 2 more staff involved in an incident, calculate for each involved staff member what their level of intervention was relative to the average of all other staff who intervened during that incident.
3. Subtract the average of other staff’s intervention levels from that staff members intervention levels.
4. Repeat this process for each incident that month with 2 or more involved staff.
5. Take the average across all incidents for that staff member.

##### Engagment Score

Engagement score addresses the possibility of deliberate indifference

1. The number of physical intervention incidents for the month in which that staff member is flagged as being ‘actually involved’ are summed and divided by the number of incidents they are flagged as being either ‘not actually involved’ or ‘witness’.
2. This score will only be calculated for staff who have completed a PIR or supplemental PIR within the past year.
