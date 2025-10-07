import pytest


@pytest.fixture(scope="session")
async def temporal_env():
    from temporalio.testing import WorkflowEnvironment

    async with await WorkflowEnvironment.start_local() as env:
        yield env
