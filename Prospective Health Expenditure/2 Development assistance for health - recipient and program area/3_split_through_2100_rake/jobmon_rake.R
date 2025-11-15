#
# jobmon workflow for raking
#
# Steps:
# - instantiate tool & workflow
# - define task templates for each type of task needed
# - generate specific instances of tasks by filling in template arguments
# - add tasks to workflow and run it

library(jobmonr)

scenario <- 'reference'
run_path <- "FILEPATH"

if (!exists(CODE_DIR)) {
  ## CODE_DIR is passed to jobmon by 1_launch_raking.R
  CODE_DIR <- "FILEPATH"
}

PATHS <- list(
  dirs = list(
    code    = CODE_DIR, 
    stdout  = "FILEPATH",
    stderr  = "FILEPATH"
  )
)



PARAMS <- list(
  user     = Sys.info()["user"],
  dt_stamp = format(Sys.time(), "%Y-%m-%d_%H:%M:%S"),
  dt_id    = format(Sys.time(), "%Y%m%d%H%M%S"),
  cluster_proj = "ID",
  cores = 8L
)

default_resources = list(
  cores = PARAMS$cores,
  queue = "ID",
  runtime = "0:05:00",
  memory = "10G",
  project = PARAMS$cluster_proj,
  stdout = PATHS$dirs$stdout,
  stderr = PATHS$dirs$stderr,
  constraints = "archive"
)


# =============================================================================
#               TOOL & WORKFLOW
# =============================================================================
#
# instantiate tool and workflow
#
JM <- new.env(parent = emptyenv())

JM$tool <- jobmonr::tool(name = "dah_fcast_rake")

JM$workflow <- jobmonr::workflow(
  tool = JM$tool,
  name = paste0("dah_fcast_rake_", PARAMS$dt_stamp)
)

#
# set default compute resources for tool & workflow
#

. <- jobmonr::set_default_tool_resources(
  tool = JM$tool,
  default_cluster_name = "ID",
  resources = default_resources
)

. <- jobmonr::set_default_workflow_resources(
  workflow = JM$workflow,
  default_cluster_name = "ID",
  resources = default_resources
)


# =============================================================================
#              TASK TEMPLATES 
# =============================================================================
#
# create task templates and define any particular resource deviations
#

# RAKING
# this task rakes a single year of data
run_rake_task <-
  jobmonr::task_template(
    tool = JM$tool,
    template_name = "run_rake_task",
    command_template = paste(
      "PYTHONPATH= OMP_NUM_THREADS={n_cores}",
      "bash", file.path(PATHS$dirs$code, "FILEPATH"),
      "{data_year}",
      "{data_tag}",
      "{work_dir}",
      sep = " "
    ),
    node_args = list("data_year", "data_tag", "work_dir"),
    task_args = list(),
    op_args = list("n_cores")
  )

. <- jobmonr::set_default_template_resources(
  task_template = run_rake_task,
  default_cluster_name = "ID",
  resources = default_resources
)


# =============================================================================
#               GENERATE TASKS
# =============================================================================
message("\n***")
message("DAH Forecast Raking Jobmon Workflow")
message("***")

#
# generate a list of specific tasks to add to the workflow
#
JM$task_list <- list(
  raking = list()
)

for (yr in seq(2024, 2100)) {
  for (tag in c("chr", "sr")) {
    for(dir in c(scenario)) {
      dirr = paste0(run_path, "FILEPATH")
      
      message("** ", yr, ", scenario ", dir, ": Creating '", tag, "' task")
      task_id <- paste0("rk_", tag, "_", yr, "_", dir)
      JM$task_list$raking[[task_id]] <- 
        jobmonr::task(
          task_template = run_rake_task,
          name = task_id,
          data_year = yr,
          data_tag = tag,
          work_dir = dirr,
          n_cores = PARAMS$cores,
          max_attempts = 3
        )
      
    }
  }
}

# =============================================================================
#               RUN
# =============================================================================
for (dir in PATHS$dirs) {
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
}

#
# add tasks to workflow
JM$workflow$add_tasks(tasks = unname(unlist(JM$task_list)))
#
# run
message("Running jobmon workflow...")

run <- jobmonr::run(JM$workflow,
                    resume = FALSE,
                    seconds_until_timeout = 36000)

message("Workflow ended.")