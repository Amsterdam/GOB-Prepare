import functools
import http.client
import time
from typing import Any, Callable, Iterator, TypeVar
from urllib.error import HTTPError

import requests


class APIException(IOError):
    """API Exception."""

    pass


def post_stream(url: str, json: dict[str, str], **kwargs) -> Iterator[bytes]:
    """Post query to GraphQL Streaming API."""
    result = requests.post(url, stream=True, json=json, **kwargs)

    try:
        result.raise_for_status()
    except requests.exceptions.RequestException:
        raise APIException(f"Request failed due to API exception, response code {result.status_code}")
    return result.iter_lines()  # type: ignore[no-any-return]


F = TypeVar("F", bound=Callable[..., Any])


def retry(max_tries: int, wait: int) -> Callable[[F], F]:  # noqa: C901
    """
    Retry `func` a number of times.

    Objective:
    The 'retry' function is a decorator that can be used to wrap other functions and add retry functionality to them.
    The objective of this function is to retry a function call a specified number of times with a specified wait time
     between each try in case of certain exceptions.

    Inputs:
    - max_tries: an integer representing the maximum number of times the function call should be retried
    - wait: an integer representing the number of seconds to wait between each retry
    - func: the function to be wrapped by the decorator

    Flow:
    1. The 'retry' function takes in the 'max_tries' and 'wait' parameters and returns a decorator function.
    2. The decorator function takes in the 'func' parameter and returns an inner function.
    3. The inner function takes in any number of arguments and keyword arguments.
    4. The inner function tries to call the 'func' with the provided arguments and keyword arguments.
    5. If the call to 'func' raises an exception of type 'HTTPError' or 'http.client.HTTPException',
    the inner function increments the 'tries' counter and checks if it has exceeded the 'max_tries' limit.
    6. If the 'tries' counter has not exceeded the 'max_tries' limit, the inner function waits for
    the specified 'wait' time and then tries to call 'func' again with the same arguments and keyword arguments.
    7. If the 'tries' counter has exceeded the 'max_tries' limit, the inner function raises the exception
    that was caught from the original call to 'func'.
    8. If the call to 'func' is successful, the inner function returns the result of the call.

    """

    def inner(func: F):
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            tries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except (HTTPError, http.client.HTTPException):
                    tries += 1
                    if tries >= max_tries:
                        raise

                    time.sleep(wait)

        return wrapper

    return inner
