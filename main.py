#
'''
Import Libraries
'''
import itertools
import pandas as pd     
import numpy as np           
import os

def function(path, time_unit, nrows, name_list, time_change):
    list_dir = os.listdir(path)
    filenames = [name for name in list_dir if time_unit in name]
    for names in filenames:
        final_output = pd.DataFrame([])                                      #create en empty dataframe 
        final_test_output = pd.DataFrame([])

        print('Processing for file {}'.format(names))
        read_csv_file = pd.read_csv(os.path.join(path, names))
#        time_1m = read_csv_file['TM_1m'].unique()
        PR = list(read_csv_file['PR'].unique())
        FL = list(read_csv_file['FL'].unique())
        GP = list(read_csv_file['GP'].unique())
        for key in name_list:
            try:
                IH_PH = list(read_csv_file[key].unique())
                break
            except:
                pass
        
        for combination in itertools.product(PR, FL, GP, IH_PH):
            horizontal_output = pd.DataFrame([])
            test_output = pd.DataFrame([])
            pr_ = combination[0]
            fl_ = combination[1]
            gp_ = combination[2]
            ih_ph_ = combination[3]
            test_output['PR'] = [pr_] * nrows
            test_output['FL'] = [fl_] * nrows
            test_output['GP'] = [gp_] * nrows
            test_output[key] = [ih_ph_] * nrows
    
            condition1 = (read_csv_file['PR'] == pr_)
            condition2 = (read_csv_file['FL'] == fl_)
            condition3 = (read_csv_file['GP'] == gp_)
            condition4 = (read_csv_file[key] == ih_ph_)
            for t in time_change:
                condition5 = (read_csv_file['TM' + time_unit] == t)
                data = read_csv_file.loc[(condition1 & condition2 & condition3 & condition4 & condition5), :]
                cols = read_csv_file.columns
                            
                if len(data) != 0:
                    PT = data.sort_values(by = ['PT'], ascending = False)  
                    top_four_rows = PT.iloc[0 : nrows]                    
                    x = top_four_rows.groupby('PT')['Vol1mR'].max().reset_index()
                    output = top_four_rows.loc[top_four_rows['Vol1mR'].isin(x['Vol1mR']) & \
                                               top_four_rows['PT'].isin(x['PT']),:]
                    output = output.reset_index()
                    
                    top_rows = output[cols[nrows:]]
                    test_out = top_rows.reset_index()                
                else:
                    a = {}
                    x = {}
                    for i, c in enumerate(['index'] + list(cols)):                    
                        if (i > 0) & (i < 5):
                            a[c] = [combination[i - 1]] * nrows
                        elif i == 6:
                            a[c] = [t] * nrows
                            x[c] = [t] * nrows  
                        else:
                            a[c]=[pd.NaT] * nrows
                            x[c]=[pd.NaT] * nrows
                    output = pd.DataFrame(a)
                    test_out = pd.DataFrame(x)
                horizontal_output = pd.concat([horizontal_output, output], axis = 1)
                test_output = pd.concat([test_output, test_out], axis = 1)
            if not final_output.empty:
                final_output = pd.concat([final_output, horizontal_output], axis = 0)
                final_test_output = pd.concat([final_test_output, test_output], axis = 0)
            else:
                final_output = horizontal_output
                final_test_output = test_output
        final_output = final_output.drop(columns = ['index'])
        final_output.to_csv(os.path.join(directory, 'Trades', names), index = None)
        read_again = pd.read_csv(os.path.join(directory, 'Trades', names))
        a1, a2, a3 = len(read_again['PR'].dropna()), \
        len(read_again['PR.1'].dropna()), len(read_again['PR.2'].dropna())
        m = np.argmax([a1, a2, a3])
        if m == 0:
            col = ['PR', 'FL']
        if m == 1:
            col = ['PR.1', 'FL.1']
        if m == 2:
            col = ['PR.2', 'FL.2']
        read_again1 = read_again.sort_values(col, ascending = True)
        read_again1.to_csv(os.path.join(directory, 'Trades', names), index = None)
        final_test_output = final_test_output.drop(columns = ['index'])        
        final_test_output.to_csv(os.path.join(directory, 'Trades', 'test_' + names), index = None)
        read_test = pd.read_csv(os.path.join(directory, 'Trades', 'test_' + names))
        read_test1 = read_test.sort_values(['PR', 'FL'], ascending = True)
        read_test1.to_csv(os.path.join(directory, 'Trades', 'test_' + names), index = None)

    
'''
Create a function which selects the top four rows.
'''

if __name__ == "__main__":
    directory = os.getcwd()                                              #get current working directory
    if not os.path.isdir(os.path.join(directory, 'Trades')):             #check if folder exists
    	os.mkdir(os.path.join(directory, 'Trades'))                  #create folder if does not exists

    path = os.path.join(directory, 'output')                         #path of the dataset 
                                          #list all the directories within the path.
    time_change = ['1m-3m', '1m-5m', '1m-30m']                           #initialize the time change
    function(path, '_10m', 4, ['IH_PH', 'IL_PL'], time_change)

        
