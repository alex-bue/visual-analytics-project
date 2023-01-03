import pandas as pd
import datetime as dt

start = dt.datetime.now()
# choose whole_world, europe or germany
level = 'germany'

# path = '../../Documents/Master/Semester1/Visual_Analytics/'
path = './data/aws_data/performance/'

combined_df = pd.DataFrame()
for i in range(0, 30):
    df = pd.read_csv(f'./data/preprocessed_files/{level}/{level}_{i}.csv', sep=';')

    # remove all NA's
    # df = df.dropna(subset='name'})

    combined_df = pd.concat([combined_df, df])

combined_df.to_csv(f'./data/final_data/{level}_pre_final.csv', sep=';')

print("Successful execution in " + str(dt.datetime.now()-start))
