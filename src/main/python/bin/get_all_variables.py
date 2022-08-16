import os
import pprint as pp

# pp.pprint(dict(os.environ))
# Set Environment Variable
os.environ['envn']="TEST"
os.environ['header']='True'
os.environ['inferSchema']='True'

# Get Environment variable
envn=os.environ['envn']
header=os.environ['header']
inferSchema=os.environ['inferSchema']

# Set Other variable
appName="USA Prescriber Research Report"
current_path=os.getcwd()
staging_dim_city=current_path+'\..\staging\dimension_city'
staging_fact=current_path+'\..\staging\\fact'
