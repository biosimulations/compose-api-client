import asyncio
from typing import Any, Tuple

from compose_api_client import Client
from compose_api_client.api.results import (
    get_simulation_results_file,
    get_simulation_status,
)
from compose_api_client.api.simulation import run_simulation
from compose_api_client.models import (
    BodyRunSimulation,
    HpcRun,
    HTTPValidationError,
    JobStatus,
    SimulationExperiment,
)
from compose_api_client.types import File, Response


def _hpc_not_type_err_msg(current_status: Any) -> str:
    return f"Expected type of HpcRun when getting simulation status, instead got {type(current_status)}: {current_status}"


sleep_interval = 2


async def async_call(
    experiment_file: File,
    client: Client,
    interval: float = 1.0,
    seconds_to_wait: int = 10 * 60,
) -> Tuple[Response[HTTPValidationError], SimulationExperiment]:
    sim_experiment = await run_simulation.asyncio(
        client=client,
        interval_time=interval,
        body=BodyRunSimulation(uploaded_file=experiment_file),
    )

    if not isinstance(sim_experiment, SimulationExperiment):
        raise TypeError(
            f"Expected type of SimulationExperiment, instead got {type(sim_experiment)}: {sim_experiment}"
        )

    current_status = await get_simulation_status.asyncio(
        client=client, simulation_id=sim_experiment.simulation_database_id
    )

    num_loops = 0
    while current_status is None and num_loops < 10:
        print("Waiting for simulation to be submitted to slurm.")
        await asyncio.sleep(sleep_interval)
        current_status = await get_simulation_status.asyncio(
            client=client, simulation_id=sim_experiment.simulation_database_id
        )

    if not isinstance(current_status, HpcRun) or not isinstance(
        current_status.status, JobStatus
    ):
        raise TypeError(_hpc_not_type_err_msg(current_status))

    print("Simulation has been submitted to slurm.")
    num_loops = 0
    while current_status.status != JobStatus.COMPLETED and num_loops < (
        seconds_to_wait / sleep_interval
    ):
        await asyncio.sleep(sleep_interval)
        current_status = await get_simulation_status.asyncio(
            client=client, simulation_id=sim_experiment.simulation_database_id
        )
        num_loops += 1

        if not isinstance(current_status, HpcRun) or not isinstance(
            current_status.status, JobStatus
        ):
            raise TypeError(_hpc_not_type_err_msg(current_status))
        if current_status.status == JobStatus.FAILED:
            raise RuntimeError(f"Simulation failed: {current_status}")

        print(
            f"Waited {num_loops * sleep_interval} seconds for simulation to complete. Current status: {current_status}"
        )

    current_status = await get_simulation_status.asyncio(
        client=client, simulation_id=sim_experiment.simulation_database_id
    )

    if not isinstance(current_status, HpcRun) or not isinstance(
        current_status.status, JobStatus
    ):
        raise TypeError(_hpc_not_type_err_msg(current_status))

    if current_status.status != JobStatus.COMPLETED:
        raise RuntimeError(f"Simulation has not completed: {current_status}")

    results: Response[
        HTTPValidationError
    ] = await get_simulation_results_file.asyncio_detailed(
        client=client, simulation_id=sim_experiment.simulation_database_id
    )

    print(f"Simulation has completed.")

    if results.status_code != 200:
        raise RuntimeError(f"Could not get simulation results: {results}")
    return results, sim_experiment
