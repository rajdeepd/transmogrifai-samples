#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import os
cwd = os.getcwd()
cwd


# In[3]:


path = '../../resources/SimpleRegressionDataSet/simple_regression.csv'


# In[4]:


data_1 = pd.read_csv(path,header=None)


# In[5]:


data_1.head(n=10)


# In[12]:


data_1.columns=['population','profit']
data_1.head(n=2)


# In[ ]:


import seaborn as sns; sns.set(color_codes=True)
tips = sns.load_dataset("tips")
plot = sns.lmplot(x="population", y="profit", data=data_1)


# In[ ]:


plot = sns.plot(x="population", y="profit", data=data_1)


# In[ ]:




