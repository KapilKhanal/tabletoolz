
# coding: utf-8

# In[ ]:


from funcy import merge, join
def mutate_row(row,**kwargs):
    '''input: row,type:dictionary and keyword arguments of column:function to mutate
    output: mutated row based onfunction '''
    newRow = {col:f(row) for col,f in kwargs.items()}
    return newRow
#Mutates and then merges    
mutate = lambda df,kwargs:(merge(row,(mutate_row(row,**kwargs)))for row in df )

#defining dataframe
df = [{'Salary': 100, 'Rent': 200, 'Gas': 200},{'Salary': 10, 'Rent': 20, 'Gas': 500}]

#mutating functions:These are user defined
func1 = lambda row:2*row["Salary"]
func2 = lambda row:0.3*row["Gas"]

#if you want to mutate existing col
kwargs = {"Salary":func1}
print(list(mutate(df,kwargs)))
#if you want to create a new mutated col
print("-------New mutate column------")
kwargs2 = {"Salary_doubled":func1,"Gas_decreased":func2}
print(list(mutate(df,kwargs2)))

