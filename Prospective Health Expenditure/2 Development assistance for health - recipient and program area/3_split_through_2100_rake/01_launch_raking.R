#
# Prepare inputs for rake.py and launch jobs to run raking for each year 
#
library(data.table)
library(arrow)

CODE_DIR <- "FILEPATH"
project <- "ID"
scenario <- 'reference'

prep_data_year <- function(dt, dah_r, data_year, save_dir) {
  dah_var = paste0("dah_ref")
  
  tmp <- dt[year == data_year, .(dah = sum(get(dah_var))), by = .(src, recip)]
  tmp <- tmp[dah != 0]
  
  ## create data frames for inner cells and the two margins
  cells <- data.table::CJ(
    year = data_year,
    src = unique(tmp$src),
    recip = unique(tmp$recip)
  )
  cells <- merge(
    cells,
    dt[, .(year, src, recip, value = get(dah_var))],
    by = c("year", "src", "recip"),
    all.x = TRUE
  )
  setnafill(cells, fill = 0, cols = "value")
  
  margin1 <- dah_r[year == data_year, .(year, recip, value_agg_over_src = get(dah_var))]
  margin2 <- cells[, .(value_agg_over_recip = sum(value)),
                   by = .(year, src)]
  
  arrow::write_feather(margin1,
                       file.path(save_dir, paste0("FILEPATH")))
  arrow::write_feather(margin2,
                       file.path(save_dir, paste0("FILEPATH")))
  arrow::write_feather(cells,
                       file.path(save_dir, paste0("FILEPATH")))
  
  ## return to inspect
  return(list(
    margin1 = margin1,
    margin2 = margin2,
    cells = cells
  ))
}

# Run data prep for each scenario
for(scenario in scenario) {
  if(scenario == "reference") {
    WORK_DIR <- paste0("FILEPATH")
  } else {
    WORK_DIR <- paste0("FILEPATH", scenario, "FILEPATH")
  }
  WORK_DIR <- paste0(WORK_DIR, "FILEPATH")
  dir.create(WORK_DIR, showWarnings = FALSE)
  
  SAVE_DIR <- file.path(WORK_DIR, "FILEPATH")
  dir.create(SAVE_DIR, showWarnings = FALSE, recursive = TRUE)
  
  RAKE_DIR <- file.path(WORK_DIR, "FILEPATH")
  dir.create(RAKE_DIR, showWarnings = FALSE, recursive = TRUE)
  
  
  #
  # Load inputs
  #
  
  # Inputs:
  dah_chr <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
  setDT(dah_chr)
  
  dah_sr <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
  setDT(dah_sr)
  
  dah_r <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
  setDT(dah_r)
  
  dah_r_resid <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
  setDT(dah_r_resid)
  
  #
  # Prep directories / remove existing data files
  #
  
  ## CHR
  ## inputs for raking
  chr_input_dir <- file.path(SAVE_DIR, "FILEPATH")
  dir.create(chr_input_dir, showWarnings = FALSE, recursive = TRUE)
  for (f in dir(chr_input_dir, full.names = TRUE)) file.remove(f)
  ## outputs of raking
  chr_raked_dir <- file.path(RAKE_DIR, "FILEPATH")
  dir.create(chr_raked_dir, showWarnings = FALSE, recursive = TRUE)
  
  ## SR
  ## inputs for raking
  sr_input_dir <- file.path(SAVE_DIR, "FILEPATH")
  dir.create(sr_input_dir, showWarnings = FALSE, recursive = TRUE)
  for (f in dir(sr_input_dir, full.names = TRUE)) file.remove(f)
  ## outputs of raking
  sr_raked_dir <- file.path(RAKE_DIR, "FILEPATH")
  dir.create(sr_raked_dir, showWarnings = FALSE, recursive = TRUE)
  
  #
  # Save raking inputs
  #
  message("* Saving data for rake.py")
  
  ## for each year, save input data needed for each task (chr, sr)
  for (yr in 2024:2100) {
    print(yr)
    x <- prep_data_year(
      dt = dah_chr,
      dah_r = dah_r,
      data_year = yr,
      save_dir = chr_input_dir
    )
    
    x <- prep_data_year(
      dt = dah_sr,
      dah_r = dah_r_resid,
      data_year = yr,
      save_dir = sr_input_dir
    )
    
  }
  
}


#
# Launch raking jobs via jobmon for all scenarios
#
message("* Starting Jobmon workflow")

source_env <- new.env()
source_env$CODE_DIR <- CODE_DIR
source(
  file.path(CODE_DIR, "jobmon_rake.R"),
  local = source_env
)
