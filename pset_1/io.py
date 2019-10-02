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

    """create a randomly named tempfile for atomic write"""
    temp = tempfile.NamedTemporaryFile(delete=False)
    temp.file.close()

    """check if the target already exists, if so, raise and exit
    yield the tempfile for write
    if no exception:
    flush and sync the file and rename it for atomic operation
    finally clean up possible garbage if any present due to failure
    """
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

