############################################
### Author: USERNAME
### Purpose: Main launch script for GHE forecasting pipeline
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

## Run model on data without COVID years
no_covid_model <- TRUE
source(paste0(code_path, "0_run_specifications.R"))

#-- PATHS FOR CURRENT RUN -------------------------------------

## Prep empty folders and return root folder
root_fold <- prep_model(variable, covid_comment, date, erase = F)
cov_root_fold <- copy(root_fold)

root_fold <- prep_model(variable, comment, date, erase = F)
non_cov_root_fold <- copy(root_fold)

## Save run specifications in root folder
save(
  list = c("metadata_list", "full_grid", "ensemble_metadata", "coef_priors"),
  file = paste0(root_fold, "FILEPATH")
)

#-- JOB INFO FOR CURRENT RUN -------------------------------------

user <- Sys.info()[["user"]]
log_dir <- paste0("FILEPATH", user)
dir.create(file.path(log_dir), showWarnings = FALSE)
dir.create(file.path(log_dir, "FILPATH"), showWarnings = FALSE)
dir.create(file.path(log_dir, "FILEPATH"), showWarnings = FALSE)

cluster <- "ID"
cluster_proj <- "ID"
queue <- "ID"
r_shell <- "FILEPATH -i FILEPATH -s"

#-- SET UP JOBMON WORKFLOW AND TEMPLATES -------------------------------------

tool <- jobmonr::tool(name=paste0('prospective_', variable))
workflow_phase1 <- jobmonr::workflow(tool=tool,
                                  workflow_args=paste0('prospective_', variable, '_wf_phase_1_', comment, '_', Sys.time()),
                                  name = paste0(toupper(variable), " forecasts"))

jobmonr::set_default_workflow_resources(
  workflow = workflow_phase1,
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

prep_task <- jobmonr::task( 
  task_template=template_prep,
  cluster_name=cluster,
  compute_resources = params_prep,
  name='prep',
  r_shell=r_shell,
  scriptname=paste0(code_path, "02_prep_covariates.R"),
  variable=variable,
  date=date,
  start_year=as.integer(start_year),
  end_fit=as.integer(end_fit),
  end_FC=as.integer(end_FC),
  release_id=as.integer(release_id),
  location_set_id=as.integer(location_set_id),
  retro_data_stats=retro_data_stats,
  retro_data_draws=retro_data_draws,
  new_retro_data_draws=new_retro_data_draws,
  prepped_inputs=prepped_inputs,
  project=project
) 

mean_forecast_tasks <- list()
for (job_num in 1:nrow(full_grid)) { 
  mean_forecast_task <- jobmonr::task(
    task_template=template_mean_forecast,
    cluster_name=cluster,
    compute_resources = params_mean_forecast,
    name=paste0(toupper(variable), "_mean_forecast_", job_num),
    r_shell=r_shell,
    scriptname=paste0(code_path, "03_mean_forecast_model.R"), 
    upstream_tasks=c(prep_task), 
    job_num=as.integer(job_num),
    variable=variable,
    date=date,
    comment=comment,
    repo_path=repo_path
  )
  mean_forecast_tasks[[job_num]] <- mean_forecast_task
}


mean_forecast_compile_task <- jobmonr::task(
  task_template=template_mean_forecast_compile,
  cluster_name=cluster,
  compute_resources = params_mean_forecast_compile,
  name='mean_forecast_compile',
  r_shell=r_shell,
  scriptname=paste0(code_path, "04_mean_forecast_model_compile.R"), 
  upstream_tasks=c(mean_forecast_tasks), 
  variable=variable,
  date=date,
  comment=comment
)


#-- ADD TASKS AND RUN WORKFLOW --------------------------------------------------------

workflow_phase1 <- jobmonr::add_tasks(workflow_phase1, c(prep_task))
workflow_phase1 <- jobmonr::add_tasks(workflow_phase1, unlist(mean_forecast_tasks, use.names=FALSE))
workflow_phase1 <- jobmonr::add_tasks(workflow_phase1, c(mean_forecast_compile_task))

status <- jobmonr::run(workflow_phase1, resume=FALSE, seconds_until_timeout=36000)

if(status == "D") {
  
  #-- SET UP JOBMON WORKFLOW AND TEMPLATES -------------------------------------
  
  # The first workflow concluded successfully. Continue on to build and run the second stage workflow
  workflow_phase2 <- jobmonr::workflow(tool=tool,
                                           workflow_args=paste0('prospective_', variable, '_wf_phase_2_', comment, '_', Sys.time()),
                                           name = paste0(toupper(variable), " forecasts"))
  
  jobmonr::set_default_workflow_resources(
    workflow = workflow_phase2,
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
  
  load(paste0(root_fold, "FILEPATH"))
  
  forecast_draws_tasks <- list()
  for (job_num in 1:nrow(rmse_distro$array_grid)) {
    forecast_draws_task <- jobmonr::task(
      task_template=template_forecast_draws,
      cluster_name=cluster,
      compute_resources = params_forecast_draws,
      name='forecast_draws',
      r_shell=r_shell,
      scriptname=paste0(code_path, "05_forecast_draws.R"), 
      job_num=as.integer(job_num),
      variable=variable,
      date=date,
      comment=comment
    )
    forecast_draws_tasks[[job_num]] <- forecast_draws_task
  }
  
  chaos_compile_tasks <- list()
  for (oos_year in 1:oos_years) {
    chaos_compile_task <- jobmonr::task(
      task_template=template_chaos_compile,
      cluster_name=cluster,
      compute_resources = params_chaos_compile,
      name='chaos_compile',
      r_shell=r_shell,
      scriptname=paste0(code_path, "06_chaos_compile.R"), 
      upstream_tasks=c(forecast_draws_tasks), 
      oos_year=as.integer(oos_year),
      variable=variable,
      date=date,
      comment=comment
    )
    chaos_compile_tasks[[oos_year]] <- chaos_compile_task
  }
  
  chaos_format_task <- jobmonr::task(
    task_template=template_chaos_format,
    cluster_name=cluster,
    compute_resources = params_chaos_format,
    name='chaos_format',
    r_shell=r_shell,
    scriptname=paste0(code_path, "07_chaos_format.R"), 
    upstream_tasks=c(chaos_compile_tasks), 
    variable=variable,
    date=date,
    comment=comment,
    repo_path=repo_path
  )
  
  derivatives_and_save_task <- jobmonr::task(
    task_template=template_derivatives_and_save,
    cluster_name=cluster,
    compute_resources = params_derivatives_and_save,
    name='derivatives_and_save',
    r_shell=r_shell,
    scriptname=paste0(code_path, "08_create_derivatives_and_save.R"), 
    upstream_tasks=c(chaos_format_task), 
    variable=variable,
    date=date,
    comment=comment
  )
  
  # end of no covid scenario tasks 
  ## From this point, we re-run everything for COVID model
  no_covid_model <- FALSE
  root_fold <- cov_root_fold
  source(paste0(code_path, "0_run_specifications.R"))
  
  ## write metadata list; other variables are the same as non-COVID model
  save(
    list = c("metadata_list", "full_grid", "ensemble_metadata", "coef_priors"),
    file = paste0(root_fold, "FILEPATH")
  )
  
  mean_forecast_tasks <- list()
  for (job_num in 1:nrow(full_grid)) { 
    mean_forecast_task <- jobmonr::task(
      task_template=template_mean_forecast,
      cluster_name=cluster,
      compute_resources = params_mean_forecast,
      name=paste0(toupper(variable), "_mean_forecast_", job_num),
      r_shell=r_shell,
      scriptname=paste0(code_path, "03_mean_forecast_model.R"), 
      upstream_tasks=c(derivatives_and_save_task), 
      job_num=as.integer(job_num),
      variable=variable,
      date=date,
      comment=comment,
      repo_path=repo_path
    )
    mean_forecast_tasks[[job_num]] <- mean_forecast_task
  }
  
  mean_forecast_compile_task <- jobmonr::task(
    task_template=template_mean_forecast_compile,
    cluster_name=cluster,
    compute_resources = params_mean_forecast_compile,
    name='mean_forecast_compile',
    r_shell=r_shell,
    scriptname=paste0(code_path, "04_mean_forecast_model_compile.R"), 
    upstream_tasks=c(mean_forecast_tasks), 
    variable=variable,
    date=date,
    comment=comment
  )
  
  #-- ADD TASKS AND RUN WORKFLOW --------------------------------------------------------
  
  # Create tasks conditionally
  workflow_phase2 <- jobmonr::add_tasks(workflow_phase2, unlist(forecast_draws_tasks, use.names=FALSE))
  workflow_phase2 <- jobmonr::add_tasks(workflow_phase2, unlist(chaos_compile_tasks, use.names=FALSE))
  workflow_phase2 <- jobmonr::add_tasks(workflow_phase2, c(chaos_format_task))
  workflow_phase2 <- jobmonr::add_tasks(workflow_phase2, c(derivatives_and_save_task))
  
  workflow_phase2 <- jobmonr::add_tasks(workflow_phase2, unlist(mean_forecast_tasks, use.names=FALSE))
  workflow_phase2 <- jobmonr::add_tasks(workflow_phase2, c(mean_forecast_compile_task))
  
  status2 <- jobmonr::run(workflow_phase2, resume=FALSE, seconds_until_timeout=36000)
  
  if(status2 == "D") {
    
    #-- SET UP JOBMON WORKFLOW AND TEMPLATES -------------------------------------
    
    workflow_phase3 <- jobmonr::workflow(tool=tool,
                                         workflow_args=paste0('prospective_", variable, "_wf_phase_3_', comment, "_", Sys.time()),
                                         name = paste0(toupper(variable), " forecasts"))
    
    jobmonr::set_default_workflow_resources(
      workflow = workflow_phase3,
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
    
    ## Get mean grid
    load(paste0(root_fold, "FILEPATH"))
    
    forecast_draws_tasks <- list()
    for (job_num in 1:nrow(rmse_distro$array_grid)) {
      forecast_draws_task <- jobmonr::task(
        task_template=template_forecast_draws,
        cluster_name=cluster,
        compute_resources = params_forecast_draws,
        name='forecast_draws',
        r_shell=r_shell,
        scriptname=paste0(code_path, "05_forecast_draws.R"), 
        job_num=as.integer(job_num),
        variable=variable,
        date=date,
        comment=comment
      )
      forecast_draws_tasks[[job_num]] <- forecast_draws_task
    }
    
    chaos_compile_tasks <- list()
    for (oos_year in 1:oos_years) {
      chaos_compile_task <- jobmonr::task(
        task_template=template_chaos_compile,
        cluster_name=cluster,
        compute_resources = params_chaos_compile,
        name='chaos_compile',
        r_shell=r_shell,
        scriptname=paste0(code_path, "06_chaos_compile.R"), 
        upstream_tasks=c(forecast_draws_tasks), 
        oos_year=as.integer(oos_year),
        variable=variable,
        date=date,
        comment=comment
      )
      chaos_compile_tasks[[oos_year]] <- chaos_compile_task
    }
    
    chaos_format_task <- jobmonr::task(
      task_template=template_chaos_format,
      cluster_name=cluster,
      compute_resources = params_chaos_format,
      name='chaos_format',
      r_shell=r_shell,
      scriptname=paste0(code_path, "07_chaos_format.R"), 
      upstream_tasks=c(chaos_compile_tasks), 
      variable=variable,
      date=date,
      comment=comment,
      repo_path=repo_path
    )
    
    derivatives_and_save_task <- jobmonr::task(
      task_template=template_derivatives_and_save,
      cluster_name=cluster,
      compute_resources = params_derivatives_and_save,
      name='derivatives_and_save',
      r_shell=r_shell,
      scriptname=paste0(code_path, "08_create_derivatives_and_save.R"), 
      upstream_tasks=c(chaos_format_task), 
      variable=variable,
      date=date,
      comment=comment
    )
    
    create_scenarios_task <- jobmonr::task(
      task_template=template_combine_scenarios,
      cluster_name=cluster,
      compute_resources = params_combine_scenarios,
      name='combine_scenarios',
      r_shell=r_shell,
      scriptname=paste0(code_path, "09_combine_COVID_scenarios.R"), 
      upstream_tasks=c(derivatives_and_save_task), 
      variable=variable,
      date=date,
      comment=comment
    )
    
    #-- ADD TASKS AND RUN WORKFLOW --------------------------------------------------------
    
    # Create tasks conditionally
    workflow_phase3 <- jobmonr::add_tasks(workflow_phase3, unlist(forecast_draws_tasks, use.names=FALSE))
    workflow_phase3 <- jobmonr::add_tasks(workflow_phase3, unlist(chaos_compile_tasks, use.names=FALSE))
    workflow_phase3 <- jobmonr::add_tasks(workflow_phase3, c(chaos_format_task))
    workflow_phase3 <- jobmonr::add_tasks(workflow_phase3, c(derivatives_and_save_task))
    workflow_phase3 <- jobmonr::add_tasks(workflow_phase3, c(create_scenarios_task))
    
    status3 <- jobmonr::run(workflow_phase3, resume=FALSE, seconds_until_timeout=36000)
    
    if(status3 == "D") {
      print("Finished all stages.")
    } else {
      stop("Failed on stage 3.")
    }
    
  } else {
    stop("Failed on stage 2")
  }
} else {
  stop("Failed on stage 1")
}

