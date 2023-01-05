# import packages
import pandas as pd
import datetime as dt

# Start timer
start = dt.datetime.now()

# choose germany or whole_world 
level = 'germany'

# combine 30 individual csv files from prior preprocessing into one csv for each Germany, Europe and whole world
combined_df = pd.DataFrame()
for i in range(0, 30):
    df = pd.read_csv(f'./data/preprocessed_files/{level}/{level}_{i}.csv', sep=';')

    combined_df = pd.concat([combined_df, df])

combined_df.drop(columns=['Unnamed: 0'], inplace=True)

combined_df.to_csv(f'./data/final_data/{level}_combined.csv', sep=';', index=False)

# End timer
print("Successful execution in " + str(dt.datetime.now()-start))
