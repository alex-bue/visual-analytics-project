# import packages
import pandas as pd
import datetime as dt

# Start timer
start = dt.datetime.now()

# combine 30 individual csv files from prior preprocessing into one csv for each Germany and the whole world
def combine_files(level):
    combined_df = pd.DataFrame()
    for i in range(0, 30):
        df = pd.read_csv(f'./data/preprocessed_files/{level}/{level}_{i}.csv', sep=';')

        combined_df = pd.concat([combined_df, df])

    # Drop index that was written to last CSV file at export
    combined_df.drop(columns=['Unnamed: 0'], inplace=True)

    # Save csv file
    combined_df.to_csv(f'./data/final_data/{level}_combined.csv', sep=';', index=False)

# Run function for whole world and germany
combine_files('whole_world')
combine_files('germany')

# End timer
print("Successful execution in " + str(dt.datetime.now()-start))
