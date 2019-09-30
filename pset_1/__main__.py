from .hash_str import get_csci_salt, get_user_id, hash_str
import pandas

def get_user_hash(username, salt=None):
    salt = salt or get_csci_salt()
    return hash_str(username, salt=salt)


if __name__ == "__main__":

    excel_path='dwb.xlsx'
    pd = pandas.read_excel(excel_path, dtype=str)
    with atomic_write_parquet(excel_path) as file:
        pd.to_parquet(file, engine='pyarrow')
    parquet_name = file.split('.')[0] + '.parquet'
    pq=pandas.read_parquet(parquet_name, engine='pyarrow')


    for user in ["gorlins", "<YOUR_GITHUB_USERNAME>"]:
        print("Id for {}: {}".format(user, get_user_id(user)))

    data_source = "data/hashed.xlsx"

    # TODO: read in, save as new parquet file, read back just id column, print
