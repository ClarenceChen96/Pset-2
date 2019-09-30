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
        f2=open(file, 'x')
        f2.close()
        yield f
    except FileExistsError:
        raise FileExistsError("The file "+str(file)+" already exists, exiting")
        exit(1)
    except:
        print("Some error occured")
        raise
        exit(1)
    finally:
        f.flush()
        os.fsync(f.fileno())
        f.close()
        os.rename(tempname, file)
        try:
            os.remove(tempname)
        except OSError as e:
            if e.errno==2:
                pass
            else:
                raise

@contextmanager
def atomic_write_parquet(file, mode="w", as_file=True, **kwargs):
    print('789')
    if '.' not in file:
        raise NameError('the file or path name is incorrect')
        #exit(0)
    else:
        if len(file.split('.'))!=2:
            raise NameError('the file or path name is incorrect')
            #exit(0)
    parquet_name=file.split('.')[0]+'.parquet'
    print(parquet_name)
    temp = tempfile.NamedTemporaryFile(delete=False)
    temp.file.close()
    try:
        print('456')
        temp=pandas.read_parquet(parquet_name)
        print('456')
        if isinstance(temp,pandas.DataFrame):
            print('123')
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