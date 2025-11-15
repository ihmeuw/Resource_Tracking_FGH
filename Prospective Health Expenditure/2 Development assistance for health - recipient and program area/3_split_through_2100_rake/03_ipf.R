#
# Purpose: Re-rake the estimates using iterative proportional fitting
# 
library(data.table)
library(arrow)
scenario <- 'reference'
project <- "ID"
if(scenario == "reference") {
  WORK_DIR <- paste0("FILEPATH")
} else {
  WORK_DIR <- paste0("FILEPATH")
}
set.seed(2718)


## Raking function
rake1way <- function(
    dt, ## data.table
    val_col, ## name of column to rake
    tot_col, ## name of column to rake TO
    by_cols, ## vector of group columns
    tol
){
  tmp <- copy(dt)
  ## rake
  tmp[,SUM := sum(get(val_col)), by = by_cols]
  tmp[,SCALAR := get(tot_col)/SUM]
  ## fix any X/0 to have scalar of 1 (no change)
  tmp[SUM == 0, SCALAR := 1]
  ## apply scalar
  tmp[,new := get(val_col)*SCALAR]
  ## check
  tmp[,SUM := sum(new), by = by_cols]
  tmp[,SCALAR := get(tot_col)/SUM]
  tmp[SUM == 0, SCALAR := 1]
  stopifnot(tmp[,max(abs(SCALAR-1))] < 1e-9)
  tmp[,c(val_col, "SUM", "SCALAR") := NULL]
  setnames(tmp, "new", val_col)
  ## return
  return(tmp)
}

## Function to perform one iteration of two way raking
two_way_rake_iter1 <- function(
    dt, 
    ## ^ data.table with two columns
    ## A, A_total, B, and B_total
    ## where we want A items to rake to A_total and B_total
    ## and we want B items to rake to A_total and B_total
    step,
    ## ^ either "out" or "back"
    ## out is raking from A items to B_total
    ## back is raking from A items to A_total
    A_group_col, B_group_col, other_group_cols,
    ## ^ names of columns
    tol
){
  if(step == "out"){
    dt <- rake1way(dt, "A", "B_total", c(A_group_col, other_group_cols), tol)
    dt <- rake1way(dt, "B", "A_total", c(B_group_col, other_group_cols), tol)
  }
  if(step == "back"){
    dt <- rake1way(dt, "A", "A_total", c(B_group_col, other_group_cols), tol)
    dt <- rake1way(dt, "B", "B_total", c(A_group_col, other_group_cols), tol)
  }
  return(dt)
}

## Function to iteratively two way rake until convergence
two_way_rake <- function(
    dt, 
    tolerance,
    max_iter, 
    ## below are col names that are passed to two_way_rake_iter1
    A_col, A_total_col, B_col, B_total_col,
    A_group_col, B_group_col, other_group_cols
){
  
  RAKED <- copy(dt)
  
  ## set names for use in tmp raking function
  setnames(
    RAKED, 
    c(A_col, A_total_col, B_col, B_total_col), 
    c("A", "A_total", "B", "B_total")
  )
  
  ## start with iteration 0 and non-convergence
  iter <- 0
  converged <- FALSE
  while (!converged && iter < max_iter) {
    
    ## Rake "out"
    prev <- copy(RAKED)
    RAKED <- two_way_rake_iter1(RAKED, "out", A_group_col, B_group_col, other_group_cols, tolerance)
    ## Track difference
    diff_out <- max(
      abs(RAKED$A - prev$A),
      abs(RAKED$B - prev$B) 
    )
    
    ## Rake "Back"
    prev <- copy(RAKED)
    RAKED <- two_way_rake_iter1(RAKED, "back", A_group_col, B_group_col, other_group_cols, tolerance)
    ## Track difference
    diff_back <- max(
      abs(RAKED$A - prev$A),
      abs(RAKED$B - prev$B) 
    )
    
    ## Get max differece
    max_diff <- max(diff_out, diff_back)
    
    # Check for convergence
    converged <- max_diff < tolerance
    iter <- iter + 1
    cat(paste0("iteration : ", iter, "\n"))
    cat(paste0("  > max_diff : ", max_diff, "\n"))
  }
  
  ## put names back to what they were
  setnames(
    RAKED, 
    c("A", "A_total", "B", "B_total"), 
    c(A_col, A_total_col, B_col, B_total_col)
  )
  
  return(RAKED)
}

#
# IPF the CHR Estimates =======================================================
#

for(scenario in scenario) {
  message(paste0("Scenario: ", scenario))
  
  dah_r <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
  setDT(dah_r)
  setnames(dah_r, paste0("dah_ref"), "dah")
  
  dah_r_resid <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
  setDT(dah_r_resid)
  setnames(dah_r_resid, paste0("dah_ref"), "dah")
  
  message("* Processing CR Estimates")
  dah_chr <- arrow::read_feather(file.path(WORK_DIR, "data", paste0("FILEPATH")))
  setDT(dah_chr)
  
  ## sum of original value (pre-rake) to get source-channel-hfa margin total
  dah_chr[, src_total := sum(value), by = .(year, src)]
  
  ## merge on dah by recipient for that margin total 
  dah_chr <- merge(
    dah_chr,
    dah_r[, .(year, recip, loc_total = dah)],
    by = c("year", "recip"),
    all.x = TRUE
  )
  
  ## use raked estimates as starting point for iterative proportional fitting
  dah_chr[, `:=`(
    loc_disagg = raked_value, src_disagg = raked_value
  )]
  
  ## use IPF to re-rake the estimates up to the margins
  ipf_chr <- two_way_rake(
    dt = dah_chr,
    tolerance = 1e-2,
    max_iter = 1000, 
    A_col = "loc_disagg", 
    A_total_col = "loc_total", 
    B_col = "src_disagg", 
    B_total_col = "src_total",
    A_group_col = "src", 
    B_group_col = "recip", 
    other_group_cols = c("year")
  )
  
  fin_chr <- ipf_chr[, .(year, src, recip,
                         orig_value = value,
                         raked1_value = raked_value,
                         ipf_value = loc_disagg)]
  
  arrow::write_feather(fin_chr, file.path(WORK_DIR, "data", paste0("FILEPATH")))
  
  fin_chr[, diff := abs(raked1_value - ipf_value)]
  message("** Max difference between raked and final value: ", max(fin_chr$diff))
  
  
  #
  # IPF the SR Estimates ========================================================
  #
  message("\n* Processing SR Estimates")
  dah_sr <- arrow::read_feather(file.path(WORK_DIR, "data", paste0("FILEPATH")))
  setDT(dah_sr)
  
  ## sum of original value (pre-rake) to get source-channel-hfa margin total
  dah_sr[, src_total := sum(value), by = .(year, src)]
  
  ## merge on dah by recipient for that margin total
  dah_sr <- merge(
    dah_sr,
    dah_r_resid[, .(year, recip, loc_total = dah)],
    by = c("year", "recip"),
    all.x = TRUE
  )
  
  ## use raked estimates as starting point for iterative proportional fitting
  dah_sr[, `:=`(
    loc_disagg = raked_value, src_disagg = raked_value
  )]
  
  ## use IPF to re-rake the estimates up to the margins
  ipf_sr <- two_way_rake(
    dt = dah_sr,
    tolerance = 1e-2,
    max_iter = 1000, 
    A_col = "loc_disagg", 
    A_total_col = "loc_total", 
    B_col = "src_disagg", 
    B_total_col = "src_total",
    A_group_col = "src", 
    B_group_col = "recip", 
    other_group_cols = c("year")
  )
  
  fin_sr <- ipf_sr[, .(year, src, recip,
                       orig_value = value,
                       raked1_value = raked_value,
                       ipf_value = loc_disagg)]
  arrow::write_feather(fin_sr, file.path(WORK_DIR, "data", paste0("FILEPATH")))
  
  fin_sr[, diff := abs(raked1_value - ipf_value)]
  message("** Max difference between raked and final value: ", max(fin_sr$diff))
}
