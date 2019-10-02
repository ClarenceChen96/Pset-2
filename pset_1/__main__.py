from .hash_str import get_csci_salt, get_user_id, hash_str
import pandas
from .io import  atomic_write
#import dotenv


def get_user_hash(username, salt=None):
    salt = salt or get_csci_salt()
    return hash_str(username, salt=salt)


if __name__ == "__main__":
    """version control:
    manually load environment variables if environment does not
    support automatic loading of dotenv file
    also un-comment import dotenv
    """
    #dotenv.load_dotenv('pset.env')

    excel_path="data/hashed.xlsx"

    """read the excel into DataFrame and write atomically to parquet"""
    pd = pandas.read_excel(excel_path, dtype=str)
    parquet_name = excel_path.split('.')[0] + '.parquet'
    with atomic_write(parquet_name, mode='wb') as file:
        pd.to_parquet(file, engine='pyarrow')

    """read back and print hashed_id column of parquet"""

    with open(parquet_name, 'rb') as f:
        pq=pandas.read_parquet(f, engine='pyarrow', columns=['hashed_id'])
    print('The student id and hashed_id is')
    for i,j in pq.iteritems():
        print(j)

    """print hashed id required in the assignment"""
    for user in ["gorlins", "ClarenceChen96"]:
        print("Id for {}: {}".format(user, get_user_id(user)))

    data_source = "data/hashed.xlsx"

    # TODO: read in, save as new parquet file, read back just id column, print
