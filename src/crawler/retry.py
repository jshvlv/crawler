import asyncio
import logging
import time
from typing import Callable, Coroutine, List, Optional, Type, Any

logger = logging.getLogger(__name__)


                              
class TransientError(Exception):
    pass


class PermanentError(Exception):
    pass


class NetworkError(Exception):
    pass


class ParseError(Exception):
    pass


class RetryStrategy:

    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        base_delay: float = 0.5,
        retry_on: Optional[List[Type[Exception]]] = None,
    ):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.base_delay = base_delay
        self.retry_on = retry_on or [TransientError, NetworkError]

                    
        self.error_stats = {
            "transient": 0,
            "network": 0,
            "permanent": 0,
            "parse": 0,
            "other": 0,
        }
        self.retry_successes = 0
        self.retry_delays: list[float] = []

    def _should_retry(self, err: Exception) -> bool:
        return any(isinstance(err, cls) for cls in self.retry_on)

    def _classify_and_count(self, err: Exception) -> None:
        if isinstance(err, TransientError):
            self.error_stats["transient"] += 1
        elif isinstance(err, NetworkError):
            self.error_stats["network"] += 1
        elif isinstance(err, PermanentError):
            self.error_stats["permanent"] += 1
        elif isinstance(err, ParseError):
            self.error_stats["parse"] += 1
        else:
            self.error_stats["other"] += 1

    async def execute_with_retry(self, coro: Callable[..., Coroutine[Any, Any, Any]], *args, **kwargs):
        attempt = 0
        last_error = None
        while attempt <= self.max_retries:
            try:
                                                                                                         
                result = await coro(*args, attempt=attempt, **kwargs)
                                                                         
                if attempt > 0:
                    self.retry_successes += 1
                return result
            except Exception as err:                                       
                self._classify_and_count(err)
                last_error = err
                                            
                if attempt >= self.max_retries or not self._should_retry(err):
                    logger.error(f"Retry failed: attempt={attempt}, error={err}")
                    raise

                                                             
                delay = self.base_delay * (self.backoff_factor ** attempt)
                self.retry_delays.append(delay)
                attempt += 1

                logger.warning(
                    f"Retrying (attempt {attempt}/{self.max_retries}) after error: {err} | waiting {delay:.2f}s"
                )
                await asyncio.sleep(delay)

                                    
        if last_error:
            raise last_error
        raise RuntimeError("execute_with_retry finished without result or error")