# pylint: disable=too-many-lines
import pytest
import requests
from conftest import assert_equal_unordered

pytestmark = [
    pytest.mark.acceptance,
]


@pytest.mark.parametrize(
    "path, verb",
    [
        ("/sources", "GET"),
        ("/sources", "HEAD"),
        ("/sources/{sourceId}", "GET"),
        ("/sources/{sourceId}", "HEAD"),
        ("/sources/{sourceId}/description", "DELETE"),
        ("/sources/{sourceId}/description", "GET"),
        ("/sources/{sourceId}/description", "HEAD"),
        ("/sources/{sourceId}/description", "PUT"),
        ("/sources/{sourceId}/label", "DELETE"),
        ("/sources/{sourceId}/label", "GET"),
        ("/sources/{sourceId}/label", "HEAD"),
        ("/sources/{sourceId}/label", "PUT"),
        ("/sources/{sourceId}/tags", "GET"),
        ("/sources/{sourceId}/tags", "HEAD"),
        ("/sources/{sourceId}/tags/{name}", "DELETE"),
        ("/sources/{sourceId}/tags/{name}", "GET"),
        ("/sources/{sourceId}/tags/{name}", "HEAD"),
        ("/sources/{sourceId}/tags/{name}", "PUT"),
    ],
)
def test_auth_401(verb, path, api_endpoint):
    # Arrange
    url = f"{api_endpoint}{path}"
    # Act
    response = requests.request(
        verb,
        url=url,
        timeout=30,
    )
    # Assert
    assert 401 == response.status_code


def test_List_Sources_HEAD_200(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Sources_HEAD_200_label(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"label": "pytest"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Sources_HEAD_200_limit(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"limit": "2"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "link" in response_headers_lower
    assert "x-paging-limit" in response_headers_lower
    assert "x-paging-nextkey" in response_headers_lower
    assert "" == response.content.decode("utf-8")


def test_List_Sources_HEAD_200_page(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"page": "1"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Sources_HEAD_200_tag_name(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"tag.test": "this"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Sources_HEAD_200_tag_exists_name(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"tag_exists.test": "false"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Sources_HEAD_400(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"format": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Sources_HEAD_400_label(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"label": "pytest", "format": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Sources_HEAD_400_limit(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"limit": "2", "format": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Sources_HEAD_400_page(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"page": "1", "format": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Sources_HEAD_400_tag_name(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"tag.test": "this", "format": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Sources_HEAD_400_tag_exists_name(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"tag_exists.test": "false", "format": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Sources_GET_200(
    api_client_cognito,
    dynamic_props,
    stub_video_source,
    stub_audio_source,
    stub_data_source,
    stub_image_source,
    stub_multi_source,
):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    for record in response_json:
        if "source_collection" in record:
            record["source_collection"] = sorted(
                record["source_collection"], key=lambda sc: sc["id"]
            )
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 5 == len(response.json())
    for prop in dynamic_props:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert {
        **stub_video_source,
        "collected_by": [stub_multi_source["id"]],
    } in response_json
    assert {
        **stub_audio_source,
        "collected_by": [stub_multi_source["id"]],
    } in response_json
    assert {
        **stub_data_source,
        "collected_by": [stub_multi_source["id"]],
    } in response_json
    assert {
        **stub_image_source,
        "collected_by": [stub_multi_source["id"]],
    } in response_json
    assert stub_multi_source in response_json


def test_List_Sources_GET_200_format(
    api_client_cognito, dynamic_props, stub_data_source, stub_multi_source
):
    """List sources with format query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"format": "urn:x-nmos:format:data"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in dynamic_props:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert_equal_unordered(
        [{**stub_data_source, "collected_by": [stub_multi_source["id"]]}], response_json
    )


def test_List_Sources_GET_200_label(
    api_client_cognito, dynamic_props, stub_multi_source
):
    """List sources with label query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"label": "pytest"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    for record in response_json:
        if "source_collection" in record:
            record["source_collection"] = sorted(
                record["source_collection"], key=lambda sc: sc["id"]
            )
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in dynamic_props:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert_equal_unordered([stub_multi_source], response_json)


def test_List_Sources_GET_200_limit(api_client_cognito):
    """List sources with limit query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"limit": "2"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "link" in response_headers_lower
    assert "x-paging-limit" in response_headers_lower
    assert "x-paging-nextkey" in response_headers_lower
    assert 2 == len(response.json())


def test_List_Sources_GET_200_page(api_client_cognito):
    """List sources with page query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"page": "1"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 4 == len(response.json())


def test_List_Sources_GET_200_tag_name(
    api_client_cognito, dynamic_props, stub_multi_source
):
    """List sources with tag.{name} query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"tag.test": "this"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    for record in response_json:
        if "source_collection" in record:
            record["source_collection"] = sorted(
                record["source_collection"], key=lambda sc: sc["id"]
            )
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in dynamic_props:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert_equal_unordered([stub_multi_source], response_json)


def test_List_Sources_GET_200_tag_exists_name(api_client_cognito):
    """List sources with tag_exists.{name} query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"tag_exists.test": "false"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 4 == len(response.json())


def test_List_Sources_GET_400(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"format": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Sources_GET_400_format(api_client_cognito):
    """List sources with format query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"format": "urn:x-nmos:format:data", "limit": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Sources_GET_400_label(api_client_cognito):
    """List sources with label query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"label": "pytest", "format": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Sources_GET_400_limit(api_client_cognito):
    """List sources with limit query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"limit": "2", "format": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Sources_GET_400_page(api_client_cognito):
    """List sources with page query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"page": "1", "format": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Sources_GET_400_tag_name(api_client_cognito):
    """List sources with tag.{name} query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"tag.test": "this", "format": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Sources_GET_400_tag_exists_name(api_client_cognito):
    """List sources with tag_exists.{name} query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"tag_exists.test": "false", "format": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Source_Details_HEAD_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Source_Details_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Source_Details_GET_200(
    api_client_cognito, dynamic_props, stub_data_source, stub_multi_source
):
    # Arrange
    path = f'/sources/{stub_data_source["id"]}'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    for prop in dynamic_props:
        if prop in response_json:
            del response_json[prop]
    assert_equal_unordered(
        {**stub_data_source, "collected_by": [stub_multi_source["id"]]}, response_json
    )


def test_Source_Details_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested Source does not exist." == response.json()["message"]


def test_List_Source_Tags_HEAD_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/tags'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Source_Tags_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/tags"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Source_Tags_GET_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/tags'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert stub_multi_source["tags"] == response.json()


def test_List_Source_Tags_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/tags"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested Source does not exist." == response.json()["message"]


def test_Source_Tag_Value_HEAD_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/tags/flow_status'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Source_Tag_Value_HEAD_404_bad_tag(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/tags/does_not_exist'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Source_Tag_Value_HEAD_404_bad_source_id(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/tags/flow_status"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Source_Tag_Value_GET_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/tags/flow_status'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert stub_multi_source["tags"]["flow_status"] == response.content.decode("utf-8")


def test_Source_Tag_Value_GET_404_bad_tag(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/tags/does_not_exist'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested Source or tag does not exist." == response.json()["message"]


def test_Source_Tag_Value_GET_404_bad_source_id(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/tags/flow_status"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested Source or tag does not exist." == response.json()["message"]


def test_Create_or_Update_Source_Tag_PUT_204_create(
    api_client_cognito, stub_multi_source
):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/tags/pytest'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Source_Tag_PUT_204_update(
    api_client_cognito, stub_multi_source
):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/tags/test'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="something else",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Source_Tag_PUT_400(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/tags/pytest'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        data="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Create_or_Update_Source_Tag_PUT_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/tags/pytest"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "The requested Source does not exist, or the tag name in the path is invalid."
        == response.json()["message"]
    )


def test_Delete_Source_Tag_DELETE_204(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/tags/pytest'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Delete_Source_Tag_DELETE_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/tags/test"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "The requested Source ID or tag in the path is invalid."
        == response.json()["message"]
    )


def test_Source_Description_HEAD_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/description'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Source_Description_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/description"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Source_Description_GET_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/description'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert stub_multi_source["description"] == response.content.decode("utf-8")


def test_Source_Description_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/description"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested Source does not exist." == response.json()["message"]


def test_Create_or_Update_Source_Description_PUT_204_create(
    api_client_cognito, stub_audio_source
):
    # Arrange
    path = f'/sources/{stub_audio_source["id"]}/description'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="pytest - audio",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Source_Description_PUT_204_update(
    api_client_cognito, stub_video_source
):
    # Arrange
    path = f'/sources/{stub_video_source["id"]}/description'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="pytest",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Source_Description_PUT_400(
    api_client_cognito, stub_video_source
):
    # Arrange
    path = f'/sources/{stub_video_source["id"]}/description'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        data="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Create_or_Update_Source_Description_PUT_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/description"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested Source does not exist." == response.json()["message"]


def test_Delete_Source_Description_DELETE_204(api_client_cognito, stub_audio_source):
    # Arrange
    path = f'/sources/{stub_audio_source["id"]}/description'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Delete_Source_Description_DELETE_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/description"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The Source ID in the path is invalid." == response.json()["message"]


def test_Source_Label_HEAD_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/label'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Source_Label_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/label"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Source_Label_GET_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f'/sources/{stub_multi_source["id"]}/label'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert stub_multi_source["label"] == response.content.decode("utf-8")


def test_Source_Label_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/label"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "The requested Source does not exist, or does not have a label set."
        == response.json()["message"]
    )


def test_Create_or_Update_Source_Label_PUT_204_create(
    api_client_cognito, stub_audio_source
):
    # Arrange
    path = f'/sources/{stub_audio_source["id"]}/label'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="pytest - audio",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Source_Label_PUT_204_update(
    api_client_cognito, stub_video_source
):
    # Arrange
    path = f'/sources/{stub_video_source["id"]}/label'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="pytest",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Source_Label_PUT_400(api_client_cognito, stub_video_source):
    # Arrange
    path = f'/sources/{stub_video_source["id"]}/label'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        data="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Create_or_Update_Source_Label_PUT_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/label"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested Source does not exist." == response.json()["message"]


def test_Delete_Source_Label_DELETE_204(api_client_cognito, stub_audio_source):
    # Arrange
    path = f'/sources/{stub_audio_source["id"]}/label'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Delete_Source_Label_DELETE_404(api_client_cognito, id_404):
    # Arrange
    path = f"/sources/{id_404}/label"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "The requested Source ID in the path is invalid." == response.json()["message"]
    )
