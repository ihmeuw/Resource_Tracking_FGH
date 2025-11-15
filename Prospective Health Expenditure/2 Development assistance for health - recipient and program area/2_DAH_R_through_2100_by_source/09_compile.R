#' - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#' @Prep: Prepare the environment
#' - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

rm(list = ls())

library(AFModel)

## Parse arguments
args <- commandArgs(trailingOnly = TRUE)
variable <- args[1]
date <- args[2]
comment <- args[3]


root_fold <- 'FILEPATH'
run_name <- paste0(date, "_", comment)

## Create directory for final files
if(!dir.exists(paste0(root_fold, "FILEPATH"))) {
  dir.create(paste0(root_fold, "FILEPATH"))
}

# Read in all draw files and append
percap_draw_files <- list.files(path = root_fold, pattern = "frax_draws.feather", ignore.case = T, recursive = T)
percap_draw_files <- percap_draw_files[percap_draw_files != "FILEPATH"]
percap_draw_files <- paste0(root_fold, "/", percap_draw_files)
percap_draws <- data.table()
for(pth in percap_draw_files) {
  percap_draw_file <- data.table(read_feather(pth))
  source <- gsub(paste0("FILEPATH"), "", pth)
  source <- gsub("FILEPATH", "", source)
  percap_draw_file[, donor := source]
  if(nrow(percap_draws) == 0){
    percap_draws <- copy(percap_draw_file)
  } else {
    percap_draws <- rbind(percap_draws, percap_draw_file)
  }
}
percap_draws <- percap_draws[, c("iso3", "year", "donor", paste0("draw_", c(1:500)))]
write_feather(percap_draws, paste0(root_fold, "FILEPATH"))

# Make stats
percap_draws <- data.table::melt(percap_draws, id.vars = c("iso3", "year", "donor"))
percap_stats <- percap_draws[, .(mean = mean(value), lower = quantile(value, 0.025), upper = quantile(value, 0.975)), 
                             by = c("iso3", "year", "donor")]
fwrite(percap_stats, paste0(root_fold, "FILEPATH"))

# Read in all draw files and append
percap_draw_files <- list.files(path = root_fold, pattern = "totes_draws.feather", ignore.case = T, recursive = T)
percap_draw_files <- percap_draw_files[percap_draw_files != "FILEPATH"]
percap_draw_files <- paste0(root_fold, "/", percap_draw_files)
percap_draws <- data.table()
for(pth in percap_draw_files) {
  percap_draw_file <- data.table(read_feather(pth))
  source <- gsub(paste0("FILEPATH"), "", pth)
  source <- gsub("FILEPATH", "", source)
  percap_draw_file[, donor := source]
  if(nrow(percap_draws) == 0){
    percap_draws <- copy(percap_draw_file)
  } else {
    percap_draws <- rbind(percap_draws, percap_draw_file)
  }
}
percap_draws <- percap_draws[, c("iso3", "year", "donor", paste0("draw_", c(1:500)))]
write_feather(percap_draws, paste0(root_fold, "FILEPATH"))

# Make stats
percap_draws <- data.table::melt(percap_draws, id.vars = c("iso3", "year", "donor"))
percap_stats <- percap_draws[, .(mean = mean(value), lower = quantile(value, 0.025), upper = quantile(value, 0.975)), 
                             by = c("iso3", "year", "donor")]
fwrite(percap_stats, paste0(root_fold, "FILEPATH"))

# Make overall metrics aggregated across donors
percap_draws_overall <- copy(percap_draws)
percap_draws_overall <- percap_draws_overall[, .(value = sum(value, na.rm = T)), by = c("iso3", "year", "variable")]
percap_draws_overall <- data.table::dcast(percap_draws_overall, iso3 + year ~ variable)
write_feather(percap_draws_overall, paste0(root_fold, "FILEPATH"))

percap_draws_overall <- data.table::melt(percap_draws_overall, id.vars = c("iso3", "year"))
percap_stats <- percap_draws_overall[, .(mean = mean(value), lower = quantile(value, 0.025), upper = quantile(value, 0.975)), 
                                     by = c("iso3", "year")]
fwrite(percap_stats, paste0(root_fold, "FILEPATH"))

# Read in all non-country donor draw files and append
nons_files <- list.files(path = root_fold, pattern = "nons", ignore.case = T, recursive = T)
nons_files <- nons_files[nons_files != "FILEPATH"]
nons_files <- paste0(root_fold, "/", nons_files)
nons <- data.table()
for(pth in nons_files) {
  nons_file <- data.table(fread(pth))
  source <- gsub(paste0("FILEPATH"), "", pth)
  source <- gsub("FILEPATH", "", source)
  nons_file[, donor := source]
  if(nrow(nons) ==0 ){
    nons <- copy(nons_file)
  } else {
    nons <- rbind(nons, nons_file)
  }
}
nons <- nons[!is.na(dah_prop_totes)]
fwrite(nons, paste0(root_fold, "FILEPATH"))
