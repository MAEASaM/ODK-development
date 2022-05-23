import pandas as pd
import numpy as np
import functools as ft


def intersection(lst1, lst2):
    """
    This private function finds interset(s) between two list. credit: 
    https://www.geeksforgeeks.org/python-intersection-two-lists/
    
    """
    lst3 = [value for value in lst1 if value in lst2]
    return lst3  

def mutliple_choice(df, column_choices=None):
    
    """
    This function reformats a table with multiple choices. It collapses all the mupltiple choices related columns to 
    a single column with all the choices
    
    """

    df_merge = []
    all_col = []
    name_all = []
    
    if column_choices is None:
        print('no multiple choice column provided')
        return df
              
    if type(column_choices) == list:
        ############
        for n in column_choices:
            #Select only columns with "site_summary/site_cultural_periods"
            chrono = [col for col in df.columns if n in col]
            all_col.append(chrono)
            #create a new dataframe from chrono

            df1 = df[chrono]

            #Remove all the choices columns (column_choices) from the parent table
            df = df.drop(chrono, axis = 1)

            # Select columns containing value true
            filter1 = (df1 == True).any()
            sub_df = df1.loc[: , filter1]
            
             #concancenate with index column
            sub_df = pd.concat([sub_df, df[['_index', '_parent_index']]], axis=1)        
            
            #Get new column names of sub_df without the index column
            new_col = sub_df.columns.values.tolist()
            new_col.pop()
            new_col 

    #         #melt the new dataframe
            df_melted = pd.melt(sub_df, id_vars=["_index", '_parent_index'], value_vars=new_col)

    #         #drop all the row with value of False
            df_melted_new = df_melted.loc[df_melted["value"] == True]

    #         #renaming the multiple choices into the column name in arches
            name = n.split('/')[-1]
            name_all.append(name)

            df_melted_new.rename(columns={"variable":name}, inplace=True)

    #         #Replace name of columns names
            df_melted_new[name] = df_melted_new[name].str.replace(n + '/','')

            df_melted_new = df_melted_new[['_index', '_parent_index', name]]

    #         #merge the ouput table to the parent table
            df_final = df.merge(df_melted_new, on = ['_index', '_parent_index'])
            df_merge.append(df_final)

        if len(df_merge) > 0:
            appended_data = ft.reduce(lambda left, right: pd.merge(left, right, on=['_index', '_parent_index']), df_merge)

            #https://stackoverflow.com/questions/23668427/pandas-three-way-joining-multiple-dataframes-on-columns

            for n in column_choices:
                chrono1 = [col for col in appended_data.columns if n in col]
                appended_data = appended_data.drop(chrono1, axis=1)

            appended_data.columns = appended_data.columns.str.replace("_x", "") #x because you will always have _x in dataframe when there           are 2 or 3 dataframes

            #Interset column names of appended_data_col and data_col
            appended_data_col = appended_data.columns.values.tolist()
            data_col = df.columns.values.tolist()

            interset = intersection(data_col, appended_data_col)
            interset.extend(name_all) #add the columns with mutliple choices

            #Only display appropriate columns
            appended_data = appended_data[interset]

            # Remove duplicate columns
            appended_data = appended_data.loc[:,~appended_data.columns.duplicated()] 

            ############
            return appended_data
        else:
            raise Exception("this (or these) column (s) do not exit in the dataframe.")
    else:
        raise Exception("double-check the parameter(s) entered,  this (there) is (are) not a list")

    

           
   
            




    
    
    
    
 
    
    
