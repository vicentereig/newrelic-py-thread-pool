import newrelic.agent
import os
import time
from structlog import get_logger
from dotenv import load_dotenv
import concurrent.futures

logger = get_logger(__name__)
load_dotenv()

worker_count = os.environ.get("WORKER_COUNT", 8)
fibo_count = os.environ.get("FIBO_COUNT", 32)
factorial_count = os.environ.get('FACTORIAL_COUNT', 50)

newrelic.agent.initialize('newrelic.ini')
application = newrelic.agent.register_application(timeout=10.0)


@newrelic.agent.background_task(name="factorial")
def factorial(n):
    # with newrelic.agent.FunctionTrace(f"factorial({n})", params={"n": n}):
    # app = newrelic.agent.register_application(timeout=10.0)
    # with newrelic.agent.BackgroundTask(app, f"factorial({n})"):
      
@newrelic.agent.background_task(name="FactorialTask")
def factorial_task(n):
    newrelic.agent.accept_distributed_trace_headers({'n': n})
    logger.info("Starts calculating Factorial number", n=n)
    delay_in_secs = 3 * n/factorial_count
    time.sleep(delay_in_secs)
    fact = factorial(n)
    logger.info("Done calculating Factorial number", n=n, f=fact, delay_in_secs=delay_in_secs)
    return fact


factorials_pool = concurrent.futures.ThreadPoolExecutor(max_workers=worker_count)
@newrelic.agent.background_task(name="FibonacciTask")
def fibonacci_task(n):
    newrelic.agent.insert_distributed_trace_headers(['n', n])
    logger.info("Starts calculating Fibonacci number", n=n)

    future_factorials = [factorials_pool.submit(factorial_task, i) for i in range(factorial_count)]
    for future_factorial in concurrent.futures.as_completed(future_factorials):
        future_factorial.result()

    fib = fibonacci(n)
    logger.info("Starts calculating Fibonacci number", n=n, fib=fib)
    return fib

@newrelic.agent.background_task(name="fibonacci")
def fibonacci(n):
    # with newrelic.agent.FunctionTrace(f"fibonacci({n})", params={"n": n}):
    # app = newrelic.agent.register_application(timeout=10.0)
    # with newrelic.agent.BackgroundTask(app, f"fibonacci({n})"):
    if n <= 1:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)

if __name__ == '__main__':
    logger.info("Scheduling Fibonacci...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(fibonacci_task, i) for i in range(fibo_count)]
        logger.info("Scheduled Fibonacci.", fibo_count=fibo_count)
        for future in concurrent.futures.as_completed(futures):
            future.result()

        logger.info("Fibonacci done.", fibonacci_result_count=fibo_count)

    factorials_pool.shutdown()
    logger.info("Shutdown factorial pool")
    newrelic.agent.shutdown_agent(timeout=2.5)
    logger.info("Shutdown NR agent")