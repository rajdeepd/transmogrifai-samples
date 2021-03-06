#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


path = '../../../../output/pimaindians/modeleval/output.csv'


# In[3]:


data = pd.read_csv(path)


# In[4]:


data.head(n=10)


# In[5]:


data.columns.values


# In[6]:


data_name_metrics = data.iloc[:,[0,4]] 
data_name_metrics_np = data_name_metrics.values


# In[7]:


data_name_metrics_np


# In[8]:


x = data_name_metrics_np[:,0]
y = data_name_metrics_np[:,1]


# In[10]:


from matplotlib import pyplot as plt 

plt.bar(x, y, align = 'center')  
plt.title('TransmogrifAI - PimaIndians, Algorithm vs Error') 
plt.ylabel('Error') 
plt.xlabel('Algorithm Name')  
plt.xticks(rotation=90)
plt.show()


# In[11]:


data_lr = data.loc[data['modelType'] == 'OpLogisticRegression']


# In[11]:


data_lr_filtered = data_lr.iloc[:,[2,4]] 


# In[12]:


data_lr_np = data_lr_filtered.values
x_lr = data_lr_np[:,0]
y_lr = data_lr_np[:,1]
data_lr_np 


# In[18]:


from matplotlib import pyplot as plt

plt.plot(x_lr,y_lr,"ob") 
plt.title('TransmogrifAI - PimaIndians, Logistic Regression') 
plt.ylabel('Error') 
plt.xlabel('RegularizationParam')  
plt.xticks(rotation=90)
plt.show()


# In[19]:


data_rf = data.loc[data['modelType'] == 'OpRandomForestClassifier']


# In[20]:


data_rf_filtered = data_rf.iloc[:,[3,4]] 


# In[21]:


data_rf_np = data_rf_filtered.values
x_rf = data_rf_np[:,0]
y_rf = data_rf_np[:,1]
data_rf_np 


# In[22]:


from matplotlib import pyplot as plt

plt.plot(x_rf,y_rf,"ob") 
plt.title('TransmogrifAI - PimaIndians, RandomForest') 
plt.ylabel('Error') 
plt.xlabel('maxDepth')  
plt.xticks(rotation=90)
plt.show()


# In[ ]:




