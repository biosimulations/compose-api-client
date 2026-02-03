from typing import Tuple

from compose_api_client import Client
from compose_api_client.api.results import (
    get_simulation_status,
    get_simulation_results_file,
)
from compose_api_client.api.simulation import run_simulation
from compose_api_client.models import (
    BodyRunSimulation,
    SimulationExperiment,
    HpcRun,
    JobStatus,
    HTTPValidationError,
)
from compose_api_client.types import File, Response
import asyncio


async def async_call(
    experiment_file: File, client: Client, seconds_to_wait: int = 10 * 60
) -> Tuple[Response[HTTPValidationError], SimulationExperiment]:
    sim_experiment = await run_simulation.asyncio(
        client=client, body=BodyRunSimulation(uploaded_file=experiment_file)
    )

    if not isinstance(sim_experiment, SimulationExperiment):
        raise TypeError()

    current_status = await get_simulation_status.asyncio(
        client=client, simulation_id=sim_experiment.simulation_database_id
    )

    if not isinstance(current_status, HpcRun) or not isinstance(
        current_status.status, JobStatus
    ):
        raise TypeError()

    num_loops = 0
    while current_status.status != JobStatus.COMPLETED and num_loops < (
        seconds_to_wait / 2
    ):
        await asyncio.sleep(2)
        current_status = await get_simulation_status.asyncio(
            client=client, simulation_id=sim_experiment.simulation_database_id
        )
        num_loops += 1

        if not isinstance(current_status, HpcRun) or not isinstance(
            current_status.status, JobStatus
        ):
            raise TypeError()
        if current_status.status == JobStatus.FAILED:
            raise RuntimeError("Simulation failed")

    current_status = await get_simulation_status.asyncio(
        client=client, simulation_id=sim_experiment.simulation_database_id
    )

    if not isinstance(current_status, HpcRun) or not isinstance(
        current_status.status, JobStatus
    ):
        raise TypeError()

    if current_status.status != JobStatus.COMPLETED:
        raise RuntimeError()

    results: Response[
        HTTPValidationError
    ] = await get_simulation_results_file.asyncio_detailed(
        client=client, simulation_id=sim_experiment.simulation_database_id
    )

    if results.status_code != 200:
        raise RuntimeError()
    return results, sim_experiment
