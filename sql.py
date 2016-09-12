


# List of folder-workflow-session ran ordered by most recent
'''
SELECT
  "ABSOLUTEFILENAME", -- binary log path
  "NODENAME",
  "INTEGRATIONSERVICE",
  "REPOSITORYDOMAIN",
  "REPOSITORYNAME",
  "RUNCONTEXT",  -- folder_id, workflow_id, session_id
  "RUNID", -- workflow_run_id, ?, ?
  "ENTRYTIME", -- starttime in milli-epoch -> datetime.datetime.fromtimestamp(1473705355958/1000.0)
  "FOLDER", -- folder text
  "WORKFLOW", -- workflow text
  "SESSIONPATH", -- session text
  "OSUSER", 
  "RUNINSTANCENAME"
FROM
  "INF_DD"."ISP_RUN_LOG"
ORDER BY ENTRYTIME desc
'''

