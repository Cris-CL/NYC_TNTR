from utils import *
import os
from dotenv import load_dotenv
import pickle as pkl

load_dotenv()
file_list = [
    'Goukei shousai 8-9.xlsx',
    'Goukei deta 8-2.xlsx',
    'Goukei shousai 8-4,5.xlsx',
    'Assis 8-2.xlsx',
    'Goukei shousai 8-10.xlsx',
    'Assis 8-3.xlsx',
    'Goukei deta 8-3.xlsx',
    'Nippo 8-7.xlsx',
    'Goukei shousai 8-8.xlsx',
    'Goukei deta 8-4,5.xlsx',
    'Goukei shousai 8-3.xlsx',
    'Goukei deta 8-8.xlsx',
    'Assis 8-8.xlsx',
    'Nippo 8-10.xlsx',
    'Assis 8-12.xlsx',
    'Assis 8-4.xlsx',
    'Assis 8-5.xlsx',
    'Nippo 8-1.xlsx',
    'Assis 8-9.xlsx',
    'Goukei deta 8-9.xlsx',
    'Goukei shousai 8-2.xlsx',
    'Goukei shousai 8-11,12.xlsx',
    'Goukei shousai 8-1.xlsx',
    'Nippo 8-2.xlsx',
    'Assis 8-10.xlsx',
    'Goukei deta 8-11,12.xlsx',
    'Assis 8-11.xlsx',
    'Nippo 8-3.xlsx',
    'Goukei deta 8-7.xlsx',
    'Assis 8-7.xlsx',
    'Nippo 8-11,12.xlsx',
    'Goukei deta 8-10.xlsx',
    'Goukei Shousai 8-7.xlsx',
    'Nippo 8-8.xlsx',
    'Nippo 8-9.xlsx',
    'Nippo 8-4,5.xlsx',
    'Goukei deta 8-1.xlsx',
    'Assis 8-1.xlsx'
    ]

tantra_path = '/Users/fujiorganics/program/Cris-CL/google_dev_env/tantra/util/tantra_data'
project_id = os.environ["PROJECT_ID"]


for file in file_list:
    # print(identify_file(file))
    if identify_file(file) == "assis":
        df = load_file(tantra_path,file)
        # df.info()
        # break
        file_name_clean = file.replace(".xlsx","")
        table = identify_file(file)
        print(table)
        # print(file_name_clean)
        try:
            upload_bq(df,f"tantra.{table}_2",project_id)
            # df.to_csv(f"util/{file_name_clean}.csv",index=False,)
        except Exception as e:
            print(df.info())
            pkl.dump(df,open(f"util/{file_name_clean}.pkl","wb"))
            # df = df.astype({"visit_time":"str"})
            # for col in df.columns:
            #     df[col].map(lambda x: print(f"{x} , {col} {type(x)}") if type(x) not in [type(""),type(1),type(1.1),""] else None)
            raise(e)
            print(e)
            print(f"error saving {file_name_clean}")
