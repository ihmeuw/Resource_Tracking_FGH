############################################
### Author: USERNAME
### Purpose: Run submodel draw estimates using TMB
############################################

rm(list = ls())

library(AFModel)

################################################
## (1) Get model information
################################################

## Open the parser
args <- commandArgs(trailingOnly = TRUE)
source <- args[1]
id <- as.numeric(args[2])
variable <- args[3]
date <- args[4]
comment <- args[5]

## Get mean grid
root_fold <- 'FILEPATH'

## Get mean grid
load(paste0(root_fold, "FILEPATH"))

### Getting model info
model_info <- get_model_specs(data = rmse_distro, model = id, draws = T)
print(paste0("Running model number ", model_info$model, " with the following specifications and ", model_info$draws, " draws"))
print(model_info)

################################################
## (2) Prep all data with covariates
################################################


## Bring in full data
input_data <- fread(gsub(".csv", paste0("_", source, ".csv"), metadata_list$prepped_inputs_path))
input_data <- input_data[year >= eval(metadata_list$start_year - 1) & year <= metadata_list$end_FC]
input_data[, year_id := as.numeric(year) - min(year)]

## Create scaled maxxer for convergence
input_data[, conv_scaler := max(get(gsub("ln\\_|logit\\_", "", model_info$yvar)), na.rm = T)]
input_data[, Y_scaled_conv := shift(get(gsub("ln\\_|logit\\_", "", model_info$yvar)) / conv_scaler), by = "iso3"]

## Drop non-country donors
input_data <- input_data[!(iso3 %in% c("OTH", "INKIND", "WLD", "UNALLOCABLE_DAH_INK", "UNALLOCABLE_DAH_WLD", "UNALLOCABLE_DAH_QZA"))]

zero_countries <- input_data[year == metadata_list$end_fit & dah_recip_total == 0, iso3]
input_data <- input_data[!(iso3 %in% as.character(zero_countries))]


################################################
## (4.1) Load previously run TMB model
################################################

load(paste0(root_fold, "FILEPATH"))


################################################
## (4.2) Prep draws of covariates and/or Y
################################################

if (model_info$draws > 0) {
  
  ### Covariates ###
  
  N_draws <- model_info$draws
  
  ##### Get draws of graduation
  random_droz <- sample(c(1:metadata_list$N_draws), size = model_info$draws, replace = F)
  grad_draws <- data.table(read_feather(metadata_list$grads_path,
                                        columns = c('iso3','year',  paste0('draw_', random_droz ) )))
  
  grad_draws <- grad_draws[year  %in% c(eval(metadata_list$start_year):metadata_list$end_FC)]
  grad_draws <- melt(grad_draws, c('iso3', 'year'), variable.name = 'draw', value.name = 'gradder')
  grad_draws[, draw:= NULL]
  grad_draws[, draw:= c(1:model_info$draws), by = c('iso3', 'year')]
  
  if (!is.null(data_params$specifications$xvar)) {
    
    #### Get the x_full (levels)
    X_level <- data.table(dcast(melt(data_params$data$x_full), Var2 + Var3 ~ Var1, value.var = "value"))
    colnames(X_level)[1:2] <- c("iso3", "year")
    
    #### Get the x_diff (diffs)
    X_diff <- data.table(dcast(melt(data_params$data$x_diff), Var2 + Var3 ~ Var1, value.var = "value"))
    colnames(X_diff)[1:2] <- c("iso3", "year")
    
  } else {
    X_diff <- NULL
    X_level <- NULL
  }
}


################################################
## (4.3) Create forecasts draws!
################################################


### Simulate parameter draws
use_prec_mat <- (output_TMB$FC_model$jointPrecision)

param_draws <- rmvnorm_prec(
  mu = output_TMB$mean_params,
  prec = use_prec_mat,
  n.sims = N_draws
)

### Create predictions

registerDoParallel(cores=10)
draws <- data.table()
for(d in 1:N_draws) {
  if (d %% 10 == 0) print(paste0("Draw = ", d))
  
  if (!is.null(data_params$specifications$xvar)) {
    X_diff[, draw := d]
    X_level[, draw := d]
  }
  
  ### Make a dataset for estimation data with the draw
  est_data <- prep_data_for_forecast(
    tmb_obj = output_TMB$obj,
    tmb_data_param = data_params,
    draw_Xdata = prepping_X_for_draw(X_diff, X_level,
                                     tmb_data_param = data_params, d = d
    ),
    draw_Ydata = NULL
  )
  
  if(model_info$int_decay == 1) {
    
    ###### RE Decay :
    ## Decay out random effects
    ## First year of decay: end_fit + number of time periods - decay factor year
    ## Last year of decay: end_fit + number of time periods + decay factor year
    ## Decay effective number of years = decay factor year*2
    
    decay_factor <- 20
    
    est_data[year>= (metadata_list$end_fit + (metadata_list$end_fit - metadata_list$start_year + 1) - decay_factor), decay_fac:= 1:.N, by = 'iso3']
    est_data[!is.na(decay_fac), z_int:= z_int*(((decay_factor*2)-decay_fac)/(decay_factor*2))]
    est_data[year>= (metadata_list$end_fit+ (metadata_list$end_fit - metadata_list$start_year + 1) + decay_factor), z_int:= 0]
  }
  
  ### Forecast with RW errors
  draw_forecast <- make_forecast(
    mean_est_data_obj = est_data,
    tmb_output_object = output_TMB,
    tmb_data_param = data_params,
    add_residual = F,
    var_metric = "mad",
    cap_variance = T,
    transform = "invlogit",
    var_threshold = if (data_params$specifications$fd == 1) {
      rmse_distro$diff_mad_cap
    } else {
      rmse_distro$level_mad_cap
    }
  )[, draw := d]
  
  draws <- rbind(draws, draw_forecast, fill = T)
}

if (!is.null(data_params$specifications$xvar)) {
  X_diff[, draw := NULL]
  X_level[, draw := NULL]
}

stopImplicitCluster()


################################################
## (4) Graduate recipients (in level space)
################################################


draws[, ydiff:= NULL]
draws <- merge(draws, grad_draws, c('iso3', 'year', 'draw'), all.x=T)
draws[, yvar:= invlogit(yvar)-(1e-10)]
draws[year > metadata_list$end_fit, yvar:= yvar*gradder]
draws[, yvar:= logit(yvar+1e-10)]

draws[is.nan(yvar), yvar:= logit(1e-10)]


draws[, gradder:= NULL]
setkeyv(draws, c('iso3', 'draw', 'year'))
draws[, ydiff:= yvar - shift(yvar), by = c('iso3', 'draw')]


################################################
## (6) Output draws
################################################


## Cast wide on draw, tag model number, and save out Rdata
out_draws <- prep_draw_output(draws, model = model_info$model)


## Save the draws
write_feather(
  out_draws,
  paste0(root_fold, "FILEPATH")
)


q("no")