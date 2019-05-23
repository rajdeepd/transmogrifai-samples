#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd


# In[3]:


path = '../../../../output/simple_regression/modeleval/output.csv'


# In[4]:


data = pd.read_csv(path)


# In[5]:


data.head(n=100)


# In[6]:


data.columns.values


# In[21]:


data_name_metrics = data.iloc[:,[0,5]] 
data_name_metrics_np = data_name_metrics.values


# In[22]:


data_name_metrics_np


# In[23]:


x = data_name_metrics_np[:,0]
y = data_name_metrics_np[:,1]
y


# In[33]:


#%matplotlib
from matplotlib import pyplot as plt 

plt.bar(x, y, align = 'center')  
plt.title('TransmogrifAI - Simple Regression, Algorithm vs Error') 
plt.ylabel('Error') 
plt.xlabel('Algorithm Name')  
plt.xticks(rotation=90)
fig_size = plt.rcParams["figure.figsize"]
print "Current size:", fig_size
fig_size[0] = 10
fig_size[1] = 6
plt.rcParams["figure.figsize"] = fig_size
plt.show()


# In[38]:


data_lr = data.loc[data['modelType'] == 'OpGBTRegressor']
data_lr


# In[39]:


data_lr_filtered = data_lr.iloc[:,[3,5]] 


# In[40]:


data_lr_np = data_lr_filtered.values
x_lr = data_lr_np[:,0]
y_lr = data_lr_np[:,1]
data_lr_np 


# In[42]:


from matplotlib import pyplot as plt

plt.plot(x_lr,y_lr,"ob") 
plt.title('TransmogrifAI - Simple Regression, Gradient Boosted Trees') 
plt.ylabel('Error') 
plt.xlabel('MaxDepth')  
plt.xticks(rotation=90)
plt.show()


# In[44]:


data_rf = data.loc[data['modelType'] == 'OpRandomForestRegressor']
data_rf


# In[45]:


data_rf_filtered = data_rf.iloc[:,[3,5]] 


# In[46]:


data_rf_np = data_rf_filtered.values
x_rf = data_rf_np[:,0]
y_rf = data_rf_np[:,1]
data_rf_np 


# In[47]:


from matplotlib import pyplot as plt

plt.plot(x_rf,y_rf,"ob") 
plt.title('TransmogrifAI - Simple Regression, OpRandomForestRegressor') 
plt.ylabel('Error') 
plt.xlabel('maxDepth')  
plt.xticks(rotation=90)
plt.show()


# In[ ]:




