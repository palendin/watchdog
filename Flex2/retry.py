from time import sleep

def retry(times, exceptions,delay):
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param times: The number of times to repeat the wrapped function/method
    :type times: Int
    :param Exceptions: Lists of exceptions that trigger a retry attempt
    :type Exceptions: Tuple of Exceptions
    """
    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs) # calls the functions thats being decorated
                except exceptions: # tries again when exception is thrown
                    print(
                        'Exception thrown when attempting to run %s, attempt '
                        '%d of %d' % (func, attempt, times)
                    )
                    attempt += 1
                    if attempt < times:
                        print(f"Retrying in {delay} seconds...")
                        sleep(delay)
            return func(*args, **kwargs)
        return newfn
    return decorator

