############################################
### Author: USERNAME
### Purpose: Main launch script for DAH-R forecasting pipeline
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

no_covid_model <- TRUE # first we run model on no-COVID data
source(paste0(code_path, "0_run_specifications.R"))

#-- PATHS FOR CURRENT RUN -------------------------------------

## Prep empty folders and return root folder
overall_root_fold <- 'FILEPATH'
if (!dir.exists(overall_root_fold)) {
  dir.create(paste0(overall_root_fold), recursive = T)
}
for(src in all_sources) {
  if (!'FILEPATH' %in% list.dirs(overall_root_fold)) {
    dir.create('FILEPATH', recursive = T)
  }
  root_fold <- prep_model(variable, comment, paste0(date, "_source_", src), erase = F)
  if (!paste0(root_fold, "FILEPATH") %in% list.dirs(root_fold)) {
    dir.create(paste0(root_fold, "FILEPATH"), recursive = T)
  }
  sub_fold <- paste0(overall_root_fold, "FILEPATH", src)
  ## Save run specifications in root folder
  save(
    list = c("metadata_list", "full_grid", "ensemble_metadata", "coef_priors"),
    file = paste0(sub_fold, "FILEPATH")
  )
}

version <- gsub('FILEPATH', "", overall_root_fold)

## Figure directories
if(!dir.exists(paste0(overall_root_fold, "FILEPATH"))) {
  system(paste0("mkdir ", overall_root_fold, "FILEPATH"))
}

#-- JOB INFO FOR CURRENT RUN -------------------------------------

user <- Sys.info()[["user"]]
log_dir <- 'FILEPATH'
dir.create(file.path(log_dir), showWarnings = FALSE)
dir.create(file.path(log_dir, "FILEPATH"), showWarnings = FALSE)
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

prep_tasks <- list()
for (src in all_sources) {
  prep_task_a <- jobmonr::task( 
    task_template=template_prep_a,
    cluster_name=cluster,
    compute_resources = params_prep_a,
    name='prep_a',
    r_shell=r_shell,
    scriptname=paste0(code_path, "02_prep_covariates.R"),
    source=src,
    variable=variable,
    date=date,
    comment=comment,
    start_year=as.integer(start_year),
    end_FC=as.integer(end_FC),
    prepped_inputs=prepped_inputs,
    nons_path=nons_path,
    retro_data=retro_data,
    release_id=as.integer(release_id),
    location_set_id=as.integer(location_set_id),
    project=project,
    last_retro_DAH_year=as.integer(last_retro_DAH_year),
    end_fit=as.integer(end_fit),
    split_run=split_run
  ) 
  prep_tasks <- append(prep_tasks, prep_task_a)
}

prep_task_b <- jobmonr::task( 
  task_template=template_prep_b,
  cluster_name=cluster,
  compute_resources = params_prep_b,
  name='prep_b',
  r_shell=r_shell,
  scriptname=paste0(code_path, "03_graduation_matrix.R"),
  upstream_tasks=c(prep_tasks), 
  date=date,
  project=project,
  end_FC=as.integer(end_FC),
  grads_path=grads_path,
  last_retro_DAH_year=as.integer(last_retro_DAH_year)
) 

mean_forecast_tasks <- list()
for (src in all_sources) {
  for (job_num in 1:nrow(full_grid)) { 
    mean_forecast_task <- jobmonr::task(
      task_template=template_mean_forecast,
      cluster_name=cluster,
      compute_resources = params_mean_forecast,
      name=paste0(toupper(variable), "_mean_forecast_", job_num),
      r_shell=r_shell,
      scriptname=paste0(code_path, "04_mean_forecast_model.R"), 
      upstream_tasks=c(prep_task_b), 
      source=src,
      job_num=as.integer(job_num),
      variable=variable,
      date=date,
      comment=comment,
      repo_path=repo_path
    )
    mean_forecast_tasks <- append(mean_forecast_tasks, mean_forecast_task)
  }
}


mean_forecast_compile_tasks <- list()
for (src in all_sources) {
  mean_forecast_compile_task <- jobmonr::task(
    task_template=template_mean_forecast_compile,
    cluster_name=cluster,
    compute_resources = params_mean_forecast_compile,
    name='mean_forecast_compile',
    r_shell=r_shell,
    scriptname=paste0(code_path, "05_mean_forecast_model_compile.R"), 
    upstream_tasks=c(mean_forecast_tasks), 
    source=src,
    variable=variable,
    date=date,
    comment=comment
  )
  mean_forecast_compile_tasks <- append(mean_forecast_compile_tasks, mean_forecast_compile_task)
}


#-- ADD TASKS AND RUN WORKFLOW --------------------------------------------------------

workflow_phase1 <- jobmonr::add_tasks(workflow_phase1, unlist(prep_tasks, use.names=FALSE))
workflow_phase1 <- jobmonr::add_tasks(workflow_phase1, c(prep_task_b))
workflow_phase1 <- jobmonr::add_tasks(workflow_phase1, unlist(mean_forecast_tasks, use.names=FALSE))
workflow_phase1 <- jobmonr::add_tasks(workflow_phase1, unlist(mean_forecast_compile_tasks, use.names=FALSE))

status <- jobmonr::run(workflow_phase1, resume=TRUE, seconds_until_timeout=36000) # keep seconds_until_timeout to be a few days 

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
  
  
  forecast_draws_tasks <- list()
  for (src in all_sources) {
    load(paste0(overall_root_fold, "FILEPATH", src, "FILEPATH"))
    
    for (job_num in 1:nrow(rmse_distro$array_grid)) {
      forecast_draws_task <- jobmonr::task(
        task_template=template_forecast_draws,
        cluster_name=cluster,
        compute_resources = params_forecast_draws,
        name='forecast_draws',
        r_shell=r_shell,
        scriptname=paste0(code_path, "06_forecast_draws.R"), 
        source=src,
        job_num=as.integer(job_num),
        variable=variable,
        date=date,
        comment=comment
      )
      forecast_draws_tasks <- append(forecast_draws_tasks, forecast_draws_task)
    }
  }
  
  chaos_compile_tasks <- list()
  for (oos_year in 1:oos_years) {
    for (src in all_sources) {
      chaos_compile_task <- jobmonr::task(
        task_template=template_chaos_compile,
        cluster_name=cluster,
        compute_resources = params_chaos_compile,
        name='chaos_compile',
        r_shell=r_shell,
        scriptname=paste0(code_path, "07_chaos_compile.R"), 
        upstream_tasks=c(forecast_draws_tasks), 
        source=src,
        oos_year=as.integer(oos_year),
        variable=variable,
        date=date,
        comment=comment
      )
      chaos_compile_tasks <- append(chaos_compile_tasks, chaos_compile_task)
    }
  }
  
  chaos_format_tasks <- list()
  for (src in all_sources) {
    chaos_format_task <- jobmonr::task(
      task_template=template_chaos_format,
      cluster_name=cluster,
      compute_resources = params_chaos_format,
      name='chaos_format',
      r_shell=r_shell,
      scriptname=paste0(code_path, "08_chaos_format.R"), 
      upstream_tasks=c(chaos_compile_tasks), 
      source=src,
      variable=variable,
      date=date,
      comment=comment,
      split_run=split_run,
      nons_path=nons_path
    )
    chaos_format_tasks <- append(chaos_format_tasks, chaos_format_task)
  }
  
  # Compile results from all climate draw runs into 1 file
  compile_task <- jobmonr::task(
    task_template=template_compile,
    cluster_name=cluster,
    compute_resources = resources_compile,
    name=paste0(toupper(variable), '_compile'),
    r_shell=r_shell,
    scriptname=paste0(code_path, "09_compile.R"), 
    upstream_tasks=unlist(chaos_format_tasks), 
    variable=variable,
    date=date,
    comment=comment
  )
  
  workflow_phase2 <- jobmonr::add_tasks(workflow_phase2, unlist(forecast_draws_tasks, use.names=FALSE))
  workflow_phase2 <- jobmonr::add_tasks(workflow_phase2, unlist(chaos_compile_tasks, use.names=FALSE))
  workflow_phase2 <- jobmonr::add_tasks(workflow_phase2, unlist(chaos_format_tasks, use.names=FALSE))
  workflow_phase2 <- jobmonr::add_tasks(workflow_phase2, c(compile_task))
  
  status2 <- jobmonr::run(workflow_phase2, resume=FALSE, seconds_until_timeout=36000) 
  
  
  if(status2 == "D") {
    stop("Finished all stages.")
  } else {
    stop("Failed on stage 2.")
  }
} else {
  stop("Failed on stage 1.")
}

