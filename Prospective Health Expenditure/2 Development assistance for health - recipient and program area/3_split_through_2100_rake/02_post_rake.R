#
# Combine raking results once all raking jobs are finished
#
library(data.table)
library(arrow)

scenario <- 'reference'
project <- "ID"
if(scenario == "reference") {
  WORK_DIR <- paste0("FILEPATH")
} else {
  WORK_DIR <- paste0("FILEPATH", scenario, "FILEPATH")
}

for(scenario in scenario) {
  RAKE_DIR <- file.path(WORK_DIR, paste0("rake_", scenario), "FILEPATH")
  
  dah_chr <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
  setDT(dah_chr)
  
  dah_sr <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
  setDT(dah_sr)
  
  dah_sr_resid <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
  setDT(dah_sr_resid)
  
  dah_r <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
  setDT(dah_r)
  
  dah_r_resid <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
  setDT(dah_r_resid)
  
  raked_chr <- data.table()
  for (yr in 2024:2100) {
    raked_chr <- rbind(
      raked_chr,
      arrow::read_feather(file.path(RAKE_DIR, "chr", paste0("FILEPATH")))
    )
  }
  raked_chr <- raked_chr[, .(year, src, recip, value, raked_value)]
  
  raked_sr <- data.table()
  for (yr in 2024:2100) {
    raked_sr <- rbind(
      raked_sr,
      arrow::read_feather(file.path(RAKE_DIR, "sr", paste0("FILEPATH")))
    )
  }
  raked_sr <- raked_sr[, .(year, src, recip, value, raked_value)]
  
  message("* Saving compiled raked data")
  arrow::write_feather(
    raked_chr,
    file.path(WORK_DIR, "data", paste0("FILEPATH"))
  )
  arrow::write_feather(
    raked_sr,
    file.path(WORK_DIR, "data", paste0("FILEPATH"))
  )
  