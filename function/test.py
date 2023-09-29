from utils import *
import os
from dotenv import load_dotenv
import pickle as pkl

load_dotenv()
file_list = os.listdir("util/28_with_col")
tantra_path = '/Users/fujiorganics/program/Cris-CL/google_dev_env/tantra/util/28_with_col'
project_id = os.environ["PROJECT_ID"]
print(file_list)
file_list = ["NYC_20230905_GSH.xlsx"]
for file in file_list:
    print(identify_file(file))
    if identify_file(file) == "shosai":
        path = f"{tantra_path}/{file}"
        df = load_file(path,file)
        df.to_csv(f"util/{file.replace('.xlsx','.csv')}",index=False)
        break
#         file_name_clean = file.replace(".xlsx","")
#         table = identify_file(file)
#         print(table)
#         # print(file_name_clean)
#         try:
#             upload_bq(df,f"tantra.{table}_2",project_id)
#             # df.to_csv(f"util/{file_name_clean}.csv",index=False,)
#         except Exception as e:
#             print(df.info())
#             pkl.dump(df,open(f"util/{file_name_clean}.pkl","wb"))
#             # df = df.astype({"visit_time":"str"})
#             # for col in df.columns:
#             #     df[col].map(lambda x: print(f"{x} , {col} {type(x)}") if type(x) not in [type(""),type(1),type(1.1),""] else None)
#             raise(e)
#             print(e)
#             print(f"error saving {file_name_clean}")
