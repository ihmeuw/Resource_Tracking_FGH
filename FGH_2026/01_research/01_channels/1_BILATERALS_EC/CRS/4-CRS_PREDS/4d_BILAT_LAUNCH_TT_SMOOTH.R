#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Launch CRS TT_Smooth by HFA
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, readstata13)
# Variable prep
data_lag <- abs(dah.roots$report_year - get_dah_param('CRS', 'data_year'))
#----------------------------# ####


cat('\n\n')
cat(green(' #####################################\n'))
cat(green(' #### CRS LAUNCH TT_SMOOTH BY HFA ####\n'))
cat(green(' #####################################\n\n'))


cat('  Read in bilateral preds\n')
#----# Read in bilateral preds #----# ####
dt <- fread(get_path('BILAT_PREDICTIONS', 'fin',
                     'Bilateral_predictions_[report_year]_[crs.update_mmyy].csv'))
#-----------------------------------# ####

cat('  Clean data for predictions\n')
#----# Clean data for predictions #----# ####
dt <- dt[ngo != 3, ]

dt[YEAR >= get_dah_param('CRS', 'data_year'),
   eval(paste0('OUTFLOW_', dah.roots$abrv_year)) := get(paste0('final_prediction_', dah.roots$abrv_year))]
setnames(dt, paste0('OUTFLOW_', dah.roots$abrv_year), paste0('DAH_', dah.roots$abrv_year))

dt[ngo == 1, ISO3 := paste0(ISO3, '_INTLNGO')]
dt[ngo == 2 & ISO3 == "USA", ISO3 := paste0(ISO3, '_USANGO')]
dt[ngo == 2 & !(ISO3 %like% 'USA'), ISO3 := paste0(ISO3, '_INTLNGO')]

dt <- dt[, lapply(.SD, sum, na.rm = TRUE),
         by = c("YEAR", "ISO3"),
         .SDcols = grep("DAH", names(dt), value = TRUE)]

# Set prelim estimates as NA
for (col in names(dt)[names(dt) %like% paste0('_DAH_', dah.roots$abrv_year)]) {
  if (data_lag == 2) {
    dt[YEAR >= dah.roots$prev_report_year, eval(col) := as.numeric(NA)]
    dt[YEAR < dah.roots$prev_report_year & is.na(get(col)), eval(col) := 0]
  }
   else if (data_lag == 1) {
     dt[YEAR == dah.roots$report_year, eval(col) := as.numeric(NA)]
     dt[YEAR <= dah.roots$prev_report_year & is.na(get(col)), eval(col) := 0]
   }
}

rm(col)
#--------------------------------------# ####

cat('  Prepare & launch TT_smooth\n')
#----# Prepare & launch TT_smooth #----# ####
iso <- unique(dt$ISO3)

all_hfas <- names(dt)[names(dt) %like% '_DAH_']
tt_cols <- paste0("pr_", all_hfas) # colnames of cols to be added by TT smooth

# Clean out old TT_smooth files
dir.create(get_path('CRS', 'int', 'tt_smooth_datasets/'), showWarnings = FALSE)
to_delete <- list.files(get_path('CRS', 'int', 'tt_smooth_datasets/'))
for (file in to_delete) {
  unlink(paste0(get_path('CRS', 'int'), 'tt_smooth_datasets/', file))
}
rm(file, to_delete)

# Create logs folder if it doesn't exist
dir.create(paste0(dah.roots$h, 'crs_tt_smooth_logs'), showWarnings = FALSE)

for (c in sort(iso)) {
  t <- dt[ISO3 == c, ]
  if (sum(t[, get(paste0("DAH_", dah.roots$abrv_year))]) == 0) {
      cat(red("Warning: ISO", c, "has no observations to send to TT Smooth\n"))
      t[, `:=`(sum_hfa = 0, diff = 0)]
      t[, eval(paste0('other_DAH_', dah.roots$abrv_year)) := 0]
      post_smooth <- matrix(0, nrow = nrow(t), ncol = length(tt_cols),
                            dimnames = list(NULL, tt_cols))
      t_post = cbind(t, post_smooth)
      # Save pre_tt_smooth dataset
      save.dta13(t, paste0(get_path('CRS', 'int'), 'tt_smooth_datasets/crs_', tolower(c), '_pre_tt_smooth.dta'))
      # Save fake post_tt_smooth dataset
      save.dta13(t_post, paste0(get_path('CRS', 'int'), 'tt_smooth_datasets/crs_', tolower(c), '_post_tt_smooth.dta'))
      next()
  }
  cat(paste0('    Prepping & launching ', c, '\n'))
  ## ensure that the sum of all hfa is equal to the total DAH
  t[, sum_hfa := rowSums(.SD, na.rm = TRUE), .SDcols = all_hfas]
  t[, (all_hfas) := lapply(.SD, \(x) get(paste0("DAH_", dah.roots$abrv_year)) * x / sum_hfa),
    .SDcols = all_hfas]
  t[, sum_hfa := NULL]
  
  # Save pre_tt_smooth dataset
  save.dta13(t, paste0(get_path('CRS', 'int'), 'tt_smooth_datasets/crs_', tolower(c), '_pre_tt_smooth.dta'))
  create_TT_config(data_path = paste0(get_path('CRS', 'int'), 'tt_smooth_datasets/crs_', tolower(c), '_'), 
                   channel_name = paste0('crs_', c),
                   total_colname = paste0('DAH_', dah.roots$abrv_year),
                   year_colname = 'YEAR', 
                   num_yrs_forecast = data_lag,
                   is_test = 0,
                   hfa_list = all_hfas)
  
  
  # launch tt_smooth
  launch_TT_smooth(channel_name = paste0('crs_', c),
                   runtime = '00:01:00', job_mem = 1,
                   errors_path = paste0(dah.roots$h, 'crs_tt_smooth_logs/', tolower(c), '.err'),
                   output_path = paste0(dah.roots$h, 'crs_tt_smooth_logs/', tolower(c), '.out'))
  rm(t)
  cat('\n')
}
#--------------------------------------# ####