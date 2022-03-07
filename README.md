# Spark with Python API (PySpark)
Correlation coefficient for continuous and categorial variables

from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pandas as pd
import numpy as np

df = sqlContext.sql("select t1.* ,t2.unemploy_rate from frmmodelling_rstr.ALS t1 LEFT JOIN frmmodelling_rstr.econ t2 ON t1.REPORT_DATE = t2.REPORT_DATE where t1.Prod_name = 'Refinance Collections' and t1.Year >= 2008 and t1.Year <= 2019")

num_input = []
cat_input = []
for x, t in df.dtypes:
	if t in ['int','double','float']:
		num_input += [x]
	elif t in ['string']:
		cat_input += [x]

cat_input = ['PREV_DELINQ','FICO_RANGE','APR_RANGE','CRPPRDID','TERM_RANGE','Month']

#Dummy encoding
for cat in cat_input:
	print(cat)
	l_vals = [v[cat] for v in df.select(cat).distinct().collect()]
	for v in l_vals:
		print(v)
		df = df.withColumn('DUMMY_'+str(cat)+'_'+str(v),when(df[cat]==v,1).otherwise(0))

d_corr = []
corr_var = num_input + [c for c in df.columns if 'DUMMY' in c]
for variable in corr_var:
	d = {}
	d['Variable'] = variable
	d['Correlation'] = df.corr('DEFAULT_IND',variable)
	d_corr += [d]

results = pd.DataFrame.from_dict(d_corr).sort_values(by='Correlation',ascending=False)
print(results)
