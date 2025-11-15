# @Purpose: 
# Fits the SFA and then extracts retrospective inefficiencies by country-year

import argparse
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys

# Load Python SFA package
sys.path.append('FILEPATH')
from pysfa import *
  
root_fold = f'FILEPATH'
file_path = f'{root_fold}/FILEPATH'
df = pd.read_csv(file_path, encoding='latin')
df3 = pd.read_csv(f'{root_fold}/FILEPATH', encoding='latin')

m = df.shape[0]
s = df['test_variance'].values*0.5
z = np.ones((m,1))
d = np.ones((m,1))

###############################################################
# --- Lower Frontier Model (negative log) ---
###############################################################
x = df['negative_log_gdppc'].values # independent variable
y = df['negative_log_ODA_per_GNI'].values # dependent variable

ind = np.argsort(x)
x = x[ind]
y = y[ind]

sfa = SFA(x.reshape(m,1), z, d, s, Y=y, add_intercept_to_x=True)

# add splines
knots = np.array([np.min(x), -11.25, -10.76357, -10.50511, -10.16451, np.max(x)])
degree = 3
sfa.addBSpline(knots, degree, l_linear=True, r_linear=True, bspline_mono='increasing',bspline_cvcv='concave')

# add priors
beta_uprior = np.array([[0]*sfa.k_beta, [8.75]*sfa.k_beta])
gama_uprior = np.array([[0.0]*sfa.k_gama, [0.0]*sfa.k_gama]) # this prior on gamma forces the random effects to zero
deta_uprior = np.array([[0.0]*sfa.k_deta, [np.inf]*sfa.k_deta]) # use a positive prior on delta


sfa.addUPrior(beta_uprior, gama_uprior, deta_uprior)
sfa.optimizeSFA()

# Optimize and trim 5% of outliers
sfa.optimizeSFAWithTrimming(int(0.90*sfa.N), stepsize=100.0, verbose=True, max_iter=20)

id_outliers_lower = np.where(sfa.w == 0.0)[0]
frontier_lower = sfa.X.dot(sfa.beta_soln)*-1

###############################################################
# --- Upper Frontier Model (positive log) ---
###############################################################
x_upper = df['log_gdppc'].values
y_upper = df['log_ODA_per_GNI'].values

ind_upper = np.argsort(x_upper)
x_upper = x_upper[ind_upper]
y_upper = y_upper[ind_upper]
s_upper = s[ind_upper]

sfa_upper = SFA(x_upper.reshape(m,1), z, d, s_upper, Y=y_upper, add_intercept_to_x=True)

knots_upper = np.array([np.min(x_upper), 10.16451, 10.50511, 10.76357, 10.92723, np.max(x_upper)])
degree = 3
sfa_upper.addBSpline(knots_upper, degree, l_linear=True, r_linear=True, bspline_mono='increasing', bspline_cvcv='concave')

gama_uprior_upper = np.array([[0.0]*sfa_upper.k_gama, [0.0]*sfa_upper.k_gama])
deta_uprior_upper = np.array([[0.0]*sfa_upper.k_deta, [np.inf]*sfa_upper.k_deta])
sfa_upper.addUPrior(gama_uprior_upper, deta_uprior_upper)

sfa_upper.optimizeSFA()
sfa_upper.optimizeSFAWithTrimming(int(0.99*sfa_upper.N), stepsize=100.0, verbose=True, max_iter=20)

id_outliers_upper = np.where(sfa_upper.w == 0.0)[0]
frontier_upper = sfa_upper.X.dot(sfa_upper.beta_soln)

###############################################################
# --- Use GDP per capita values and generate predictions on the frontiers ---
###############################################################
dt_length = df3.shape[0]

# Plug into lower frontier
X_new_lower = df3.eval(f'negative_log_gdppc').as_matrix().reshape(dt_length, 1)
v_new_lower = df3.eval(f'ineff')
v_new_lower = v_new_lower.as_matrix()
y_new_lower = sfa.forcastData(X_new_lower, v_new_lower, add_intercept_to_x=True)

# Plug into lower frontier
X_new_upper = df3.eval(f'log_gdppc').as_matrix().reshape(dt_length, 1)
v_new_upper = df3.eval(f'ineff')
v_new_upper = v_new_upper.as_matrix()
y_new_upper = sfa_upper.forcastData(X_new_upper, v_new_upper, add_intercept_to_x=True)


# Append together preds data and save out
preds = pd.DataFrame({f'sfa_pred_lower':y_new_lower, f'sfa_pred_upper': y_new_upper})

df2 = pd.merge(df3, preds, left_index = True, right_index = True)

###############################################################
# --- Save out predictions ---
###############################################################

df2.to_csv(f'{root_fold}/FILEPATH', index=False)
