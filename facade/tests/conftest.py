from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi.testclient import TestClient

from facade.dependencies import Settings, get_docling_client, get_redis, get_s3_client, get_settings
from facade.main import app
from facade.tests.fakes import FakeRedis, FakeS3Client


@pytest.fixture
def settings() -> Settings:
    return Settings()


@pytest.fixture
def fake_s3() -> FakeS3Client:
    return FakeS3Client()


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def mock_docling_client() -> AsyncMock:
    return AsyncMock(spec=httpx.AsyncClient)


@pytest.fixture
def client(settings, fake_s3, fake_redis, mock_docling_client) -> TestClient:
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_s3_client] = lambda: fake_s3
    app.dependency_overrides[get_redis] = lambda: fake_redis
    app.dependency_overrides[get_docling_client] = lambda: mock_docling_client
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.clear()
