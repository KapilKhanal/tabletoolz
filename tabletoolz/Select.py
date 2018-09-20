
# coding: utf-8

# In[ ]:


df = [{"Name":"Kapil","Major":"Data"},{"Name":"Mapil","Major":"Hata"}]
cols = ["Name"]

def select_yield(df,cols):
    '''Input: Dataframe as list of dictionary, each dictionary as a row and list of cols to select
       Output:list of dictionary, each dictionary as a row with selected column from cols '''
    for row in df:
        for k,v in row.items():
            if k in cols:
                yield({k:v})
                

select = lambda df,cols: ({k:v} for row in df for k,v in row.items() if k in cols)
list(select(df,["Name"]))

