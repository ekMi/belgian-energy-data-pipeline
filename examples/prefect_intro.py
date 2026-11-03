from prefect import flow, task
from prefect.runtime import task_run


@task(log_prints=True)
def get_load_values() -> list[int]:
    values = [8500, 9200, 9100, 8700]
    print(f"Valeurs récupérées : {values}")
    return values


@task(log_prints=True, retries=2, retry_delay_seconds=3)
def calculate_average(values: list[int]) -> float:
    attempt = task_run.run_count
    print(f"Tentative numéro {attempt}")

    if attempt == 1:
        raise ConnectionError("Erreur réseau temporaire simulée")

    average = sum(values) / len(values)
    print(f"Charge moyenne : {average:.0f} MW")
    return average


@flow(name="energy-discovery")
def energy_flow() -> float:
    values = get_load_values()
    average = calculate_average(values)
    return average


if __name__ == "__main__":
    energy_flow()
