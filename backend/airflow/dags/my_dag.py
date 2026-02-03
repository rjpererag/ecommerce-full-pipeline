import pendulum
from airflow.sdk import dag, task


@task()
def get_current_time() -> str:
    print("Getting current time")
    return pendulum.now().isoformat()


@task()
def print_current_time(current_time: str) -> None:
    print(f"Current time_ {current_time}")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["my_dag"],
)
def my_dag():
    current_time = get_current_time()
    print_current_time(current_time)

my_dag()