df = [{"Name":"Kapil","Major":"Data"},{"Name":"Mapil","Major":"Hata"}]
cols = ["Name"]

def select_yield(df,cols):
    '''Input: Dataframe as list of dictionary, each dictionary as a row and list of cols to select
       Output:list of dictionary, each dictionary as a row with selected column from cols '''
    yield ({k:v for k,v in row.items() if k in cols} for row in df)
    
    
                
                

select = lambda df,cols: ({k:v for k,v in row.items() if k in cols} for row in df)
list(next(select_yield(df,["Name","Major"])))

