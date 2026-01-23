import pytest

pytestmark = [
    pytest.mark.acceptance,
]


@pytest.mark.parametrize(
    "path, method, scopes",
    [
        ("/service", "POST", ["tams-api/read", "tams-api/write", "tams-api/delete"]),
        ("/service/webhooks", "HEAD", ["tams-api/write", "tams-api/delete"]),
        ("/service/webhooks", "GET", ["tams-api/write", "tams-api/delete"]),
        ("/service/webhooks", "POST", ["tams-api/read", "tams-api/delete"]),
        (
            "/service/webhooks/00000000-0000-1000-8000-00000000000a",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/service/webhooks/00000000-0000-1000-8000-00000000000a",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/service/webhooks/00000000-0000-1000-8000-00000000000a",
            "PUT",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/service/webhooks/00000000-0000-1000-8000-00000000000a",
            "DELETE",
            ["tams-api/write", "tams-api/delete"],
        ),
        ("/sources", "HEAD", ["tams-api/write", "tams-api/delete"]),
        ("/sources", "GET", ["tams-api/write", "tams-api/delete"]),
        (
            "/sources/00000000-0000-1000-8000-000000000000",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/tags",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/tags",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/tags/name",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/tags/name",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/tags/name",
            "PUT",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/tags/name",
            "DELETE",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/description",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/description",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/description",
            "PUT",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/description",
            "DELETE",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/label",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/label",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/label",
            "PUT",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/sources/00000000-0000-1000-8000-000000000000/label",
            "DELETE",
            ["tams-api/read", "tams-api/delete"],
        ),
        ("/flows", "HEAD", ["tams-api/write", "tams-api/delete"]),
        ("/flows", "GET", ["tams-api/write", "tams-api/delete"]),
        (
            "/flows/10000000-0000-1000-8000-000000000000",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/tags",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/tags",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/tags/name",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/tags/name",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/tags/name",
            "PUT",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/tags/name",
            "DELETE",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/description",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/description",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/description",
            "PUT",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/description",
            "DELETE",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/label",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/label",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/label",
            "PUT",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/label",
            "DELETE",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/read_only",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/read_only",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/read_only",
            "PUT",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/flow_collection",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/flow_collection",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/flow_collection",
            "PUT",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/flow_collection",
            "DELETE",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/max_bit_rate",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/max_bit_rate",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/max_bit_rate",
            "PUT",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/max_bit_rate",
            "DELETE",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/avg_bit_rate",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/avg_bit_rate",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/avg_bit_rate",
            "PUT",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/avg_bit_rate",
            "DELETE",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/segments",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/segments",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/segments",
            "POST",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/segments",
            "DELETE",
            ["tams-api/read", "tams-api/write"],
        ),
        (
            "/flows/10000000-0000-1000-8000-000000000000/storage",
            "POST",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/objects/10000000-0000-1000-8000-00000000000a",
            "HEAD",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/objects/10000000-0000-1000-8000-000000000000a",
            "GET",
            ["tams-api/write", "tams-api/delete"],
        ),
        (
            "/objects/10000000-0000-1000-8000-000000000000a/instances",
            "POST",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/objects/10000000-0000-1000-8000-000000000000a/instances",
            "DELETE",
            ["tams-api/read", "tams-api/delete"],
        ),
        (
            "/flow-delete-requests",
            "HEAD",
            ["tams-api/read", "tams-api/write", "tams-api/delete"],
        ),
        (
            "/flow-delete-requests",
            "GET",
            ["tams-api/read", "tams-api/write", "tams-api/delete"],
        ),
        (
            "/flow-delete-requests/90000000-0000-1000-8000-000000000000",
            "HEAD",
            ["tams-api/read", "tams-api/write"],
        ),
        (
            "/flow-delete-requests/90000000-0000-1000-8000-000000000000",
            "GET",
            ["tams-api/read", "tams-api/write"],
        ),
    ],
)
def test_scope_authorization_403(api_client_factory, path, method, scopes):
    """Test that insufficient scopes result in 403 Forbidden."""
    # Arrange
    client = api_client_factory(scopes)

    # Act
    response = client.request(method, path)

    # Assert
    assert (
        response.status_code == 403
    ), f"Expected 403 for {method} {path} with scopes {scopes}, got {response.status_code}"
