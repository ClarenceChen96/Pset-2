from contextlib import contextmanager
import tempfile
import os

@contextmanager
def atomic_write(file, mode="w", as_file=True, **kwargs):
    """Write a file atomically

    :param file: str or :class:`os.PathLike` target to write

    :param bool as_file:  if True, the yielded object is a :class:File.
        (eg, what you get with `open(...)`).  Otherwise, it will be the
        temporary file path string

    :param kwargs: anything else needed to open the file

    :raises: FileExistsError if target exists

    Example::

        with atomic_write("hello.txt") as f:
            f.write("world!")

    """
    temp = tempfile.NamedTemporaryFile(delete=False)
    temp.file.close()
    try:
        tempname=temp.name
        f=open(tempname, mode, as_file, **kwargs)
        if os.path.exists(file):
            raise FileExistsError
        yield f
    except FileExistsError:
        raise FileExistsError("The file "+str(file)+" already exists, exiting")
        exit(1)
    except Exception as e:
        raise e
        exit(1)
    else:
        f.flush()
        os.fsync(f.fileno())
        f.close()
        os.rename(tempname, file)
    finally:
        try:
            os.remove(tempname)
        except OSError as e:
            if e.errno==2:
                pass
            else:
                print('unable to remove file')
                raise

@contextmanager
def atomic_write_parquet(file, mode="w", as_file=True, **kwargs):
    if '.' not in file:
        raise NameError('the file or path name is incorrect')
        #exit(0)
    else:
        if len(file.split('.'))!=2:
            raise NameError('the file or path name is incorrect')
            #exit(0)
    parquet_name=file.split('.')[0]+'.parquet'
    temp = tempfile.NamedTemporaryFile(delete=False)
    temp.file.close()
    try:
        temp=pandas.read_parquet(parquet_name)
        if isinstance(temp,pandas.DataFrame):
            raise FileExistsError('file '+parquet_name+' already exist')
            exit(0)
    except FileExistsError:
        raise
    except:
        pass
    try:
        tempname=temp.name
        yield tempname
    except Error as e:
        print("Some error occured:")
        raise e
        exit(1)
    finally:
        os.rename(tempname, parquet_name)
        try:
            os.remove(tempname)
        except OSError as e:
            if e.errno==2:
                pass
            else:
                raise