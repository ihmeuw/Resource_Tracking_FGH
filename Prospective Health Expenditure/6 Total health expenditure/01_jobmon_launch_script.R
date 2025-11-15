############################################
### Author: USERNAME
### Purpose: Main launch script for creating THE from the components 
############################################

library(jobmonr)
library(AFModel)

#-- VARIABLES FOR CURRENT RUN -------------------------------------

if(dir.exists("ADDRESS")) {
  repo_path <- "ADDRESS"
} else if(dir.exists("ADDRESS")) {
  repo_path <- "ADDRESS"
}
code_path <- paste0(repo_path, "FILEPATH")

source(paste0(code_path, "0_run_specifications.R"))

#-- PATHS AND JOB INFO FOR CURRENT RUN -------------------------------------

root_fold <- 'FILEPATH'

# Create folders for current run
dir.create(root_fold)
dir.create(paste0(root_fold, "FILEPATH"))
dir.create(paste0(root_fold, "FILEPATH"))
dir.create(paste0(root_fold, "FILEPATH"))
dir.create(paste0(root_fold, "FILEPATH"))
dir.create(paste0(root_fold, "FILEPATH"))

save(metadata_list, file = paste0(root_fold, "FILEPATH"))

#-- JOB INFO FOR CURRENT RUN -------------------------------------

user <- Sys.info()[["user"]]
log_dir <- 'FILEPATH'
dir.create(file.path(log_dir, "FILEPATH"), showWarnings = FALSE)
dir.create(file.path(log_dir, "FILEPATH"), showWarnings = FALSE)

cluster <- "ID"
cluster_proj <- "ID" 
queue <- "ID"
r_shell <- "FILEPATH -i FILEPATH -s"

#-- SET UP JOBMON WORKFLOW AND TEMPLATES -------------------------------------

tool <- jobmonr::tool(name=paste0('prospective_', variable))
workflow <- jobmonr::workflow(tool=tool,
                                  workflow_args=paste0('prospective_', variable, '_wf_phase_1_', version, '_', Sys.time()),
                                  name = paste0(toupper(variable), " forecasts"))

jobmonr::set_default_workflow_resources(
  workflow = workflow,
  default_cluster_name = cluster,
  resources = list(
    "project" = cluster_proj,
    "working_dir" = getwd(),
    "stdout" = paste0(log_dir, "FILEPATH"),
    "stderr" = paste0(log_dir, "FILEPATH")
  )
)


# load task templates and resource parameters
source(paste0(code_path, "FILEPATH"))


#-- CREATE TASKS -------------------------------------------------------------

create_the_task <- jobmonr::task(
  task_template=template_create_the,
  cluster_name=cluster,
  compute_resources = params_create_the,
  name='create_the',
  r_shell=r_shell,
  scriptname=paste0(code_path, "02_create_THE.R"),
  variable=variable,
  version=version
)

he_scenarios_task <- jobmonr::task(
    task_template=template_he_scenarios,
    cluster_name=cluster,
    compute_resources = params_he_scenarios,
    name='he_scenarios',
    r_shell=r_shell,
    scriptname=paste0(code_path, "03_HE_per_cap_scenarios.R"), 
    upstream_tasks=c(create_the_task), 
    variable=variable,
    version=version
)


panel_figures_task <- jobmonr::task(
  task_template=template_panel_figures,
  cluster_name=cluster,
  compute_resources = params_panel_figures,
  name='panel_figures',
  r_shell=r_shell,
  scriptname=paste0(code_path, "04_panel_figures.R"), 
  upstream_tasks=c(he_scenarios_task), 
  variable=variable,
  THE_version=THE_version
)


#-- ADD TASKS AND RUN WORKFLOW --------------------------------------------------------

workflow <- jobmonr::add_tasks(workflow, c(create_the_task))
workflow <- jobmonr::add_tasks(workflow, c(he_scenarios_task))
workflow <- jobmonr::add_tasks(workflow, c(panel_figures_task))

status <- jobmonr::run(workflow, resume=FALSE, seconds_until_timeout=36000) 

if (status != "D") {
  stop("The workflow failed")
} else {
  message("The workflow completed successfully!")
}

