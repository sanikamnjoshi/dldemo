# there are several ways to import (parts of) packages in python:

# importing the whole package such that its components can only be accessed using the package name
import pandas
df1 = pandas.DataFrame() # initialize an empty data frame

# importing a package and giving it an alias such that its components can only be accessed using the alias (here 'pd')
import pandas as pd
df2 = pd.DataFrame()

# importing a specific component of a package such that it can be used directly
from pandas import DataFrame
df3 = DataFrame() # initiate an empty data frame

# importing every component of a package such that it can be used directly
from pandas import *
df4 = DataFrame() # initiate an empty data frame

##########

# you can also import functions you defined yourself

from dldemo.examples.functions_example import compute_multipy, compute_sum
print(compute_multipy(13, 2))
test = compute_sum(100, 17)
print(test)