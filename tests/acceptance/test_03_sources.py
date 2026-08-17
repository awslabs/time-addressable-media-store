# pylint: disable=too-many-lines
import pytest
import requests
from conftest import (
    ID_404,
    assert_equal_unordered,
    assert_headers_present,
    assert_json_response,
    remove_dynamic_props,
)

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
    assert_json_response(response, 401)


def test_List_Sources_HEAD_200(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Sources_HEAD_200_collected_by_ids(api_client_cognito, stub_multi_source):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"collected_by_ids": stub_multi_source["id"]},
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Sources_HEAD_200_collected_by_ids_empty(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"collected_by_ids": ""},
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Sources_HEAD_200_label(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"label": "pytest"},
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Sources_HEAD_200_limit(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"limit": "2"},
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)
    assert_headers_present(
        response, "link", "x-paging-limit", "x-paging-nextkey", "x-paging-count"
    )


def test_List_Sources_HEAD_200_page(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"page": "1"},
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Sources_HEAD_200_tag_name(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"tag.test": "this"},
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Sources_HEAD_200_tag_exists_name(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"tag_exists.test": "false"},
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Sources_HEAD_400(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"format": "bad"},
    )
    # Assert
    assert_json_response(response, 400, empty_body=True)


def test_List_Sources_HEAD_400_collected_by_ids(api_client_cognito, stub_multi_source):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"collected_by_ids": stub_multi_source["id"], "format": "bad"},
    )
    # Assert
    assert_json_response(response, 400, empty_body=True)


def test_List_Sources_HEAD_400_collected_by_ids_malformed(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"collected_by_ids": "not-a-uuid"},
    )
    # Assert
    assert_json_response(response, 400, empty_body=True)


def test_List_Sources_HEAD_400_label(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"label": "pytest", "format": "bad"},
    )
    # Assert
    assert_json_response(response, 400, empty_body=True)


def test_List_Sources_HEAD_400_limit(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"limit": "2", "format": "bad"},
    )
    # Assert
    assert_json_response(response, 400, empty_body=True)


def test_List_Sources_HEAD_400_page(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"page": "1", "format": "bad"},
    )
    # Assert
    assert_json_response(response, 400, empty_body=True)


def test_List_Sources_HEAD_400_tag_name(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"tag.test": "this", "format": "bad"},
    )
    # Assert
    assert_json_response(response, 400, empty_body=True)


def test_List_Sources_HEAD_400_tag_exists_name(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"tag_exists.test": "false", "format": "bad"},
    )
    # Assert
    assert_json_response(response, 400, empty_body=True)


def test_List_Sources_GET_200(
    api_client_cognito,
    stub_video_source,
    stub_audio_source,
    stub_data_source,
    stub_image_source,
    stub_multi_source,
    stub_init_source,
):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 200)
    response_json = remove_dynamic_props(response.json())
    assert 6 == len(response_json)
    assert_equal_unordered(
        [
            stub_multi_source,
            stub_video_source,
            stub_audio_source,
            stub_data_source,
            stub_image_source,
            stub_init_source,
        ],
        response_json,
    )


def test_List_Sources_GET_200_collected_by_ids(
    api_client_cognito,
    stub_multi_source,
    stub_video_source,
    stub_audio_source,
    stub_data_source,
    stub_image_source,
):
    """List sources collected by the given Source id

    Source collection is derived from the Flow graph: a Source is collected by
    another Source when one of its Flows is collected by a Flow of that Source.
    """
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"collected_by_ids": stub_multi_source["id"]},
    )
    # Assert
    assert_json_response(response, 200)
    response_json = remove_dynamic_props(response.json())
    assert 4 == len(response_json)
    assert_equal_unordered(
        [stub_video_source, stub_audio_source, stub_data_source, stub_image_source],
        response_json,
    )


def test_List_Sources_GET_200_collected_by_ids_multiple(
    api_client_cognito,
    stub_multi_source,
    stub_video_source,
    stub_audio_source,
    stub_data_source,
    stub_image_source,
):
    """List sources collected by any of the given Source ids

    Multiple ids are OR-ed, so pairing the real collecting Source with one that
    collects nothing must not narrow the result set.
    """
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"collected_by_ids": f"{stub_multi_source['id']},{ID_404}"},
    )
    # Assert
    assert_json_response(response, 200)
    response_json = remove_dynamic_props(response.json())
    assert 4 == len(response_json)
    assert_equal_unordered(
        [stub_video_source, stub_audio_source, stub_data_source, stub_image_source],
        response_json,
    )


def test_List_Sources_GET_200_collected_by_ids_empty(
    api_client_cognito, stub_multi_source, stub_init_source
):
    """List only sources that no other Source collects

    An empty value is a filter, not an absent one: it must return the
    uncollected Sources rather than all six.
    """
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"collected_by_ids": ""},
    )
    # Assert
    assert_json_response(response, 200)
    response_json = remove_dynamic_props(response.json())
    assert 2 == len(response_json)
    assert_equal_unordered([stub_multi_source, stub_init_source], response_json)


def test_List_Sources_GET_200_format(api_client_cognito, stub_data_source):
    """List sources with format query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"format": "urn:x-nmos:format:data"},
    )
    # Assert
    assert_json_response(response, 200)
    response_json = remove_dynamic_props(response.json())
    assert 1 == len(response_json)
    assert_equal_unordered([stub_data_source], response_json)


def test_List_Sources_GET_200_label(api_client_cognito, stub_multi_source):
    """List sources with label query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"label": "pytest"},
    )
    # Assert
    assert_json_response(response, 200)
    response_json = remove_dynamic_props(response.json())
    assert 1 == len(response_json)
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
    # Assert
    assert_json_response(response, 200)
    assert_headers_present(
        response, "link", "x-paging-limit", "x-paging-nextkey", "x-paging-count"
    )
    response_json = response.json()
    assert 2 == len(response_json)


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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 5 == len(response_json)


def test_List_Sources_GET_200_sort_by_label(api_client_cognito):
    """List sources sorted by label (ascending alphabetical by default)"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"sort_by": "label"},
    )
    # Assert
    assert_json_response(response, 200)
    assert_headers_present(response, "x-paging-reverse-order")
    assert "False" == response.headers["X-Paging-Reverse-Order"]
    has_label = [source.get("label") is not None for source in response.json()]
    # label is optional; filtering absent labels preserves the relative order of
    # those present, so a correct ascending sort still holds for this subset.
    labels = [
        source["label"] for source in response.json() if source.get("label") is not None
    ]
    assert labels == sorted(labels)
    # Sources with an unset label sort after those with one by default.
    assert has_label == sorted(has_label, reverse=True)


def test_List_Sources_GET_200_sort_by_label_reverse_order(api_client_cognito):
    """List sources sorted by label in reverse (descending alphabetical)"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"sort_by": "label", "reverse_order": "true"},
    )
    # Assert
    assert_json_response(response, 200)
    assert_headers_present(response, "x-paging-reverse-order")
    assert "True" == response.headers["X-Paging-Reverse-Order"]
    has_label = [source.get("label") is not None for source in response.json()]
    # label is optional; filtering absent labels preserves the relative order of
    # those present, so a correct descending sort still holds for this subset.
    labels = [
        source["label"] for source in response.json() if source.get("label") is not None
    ]
    assert labels == sorted(labels, reverse=True)
    # Sources with an unset label sort before those with one when reversed.
    assert has_label == sorted(has_label)


def test_List_Sources_GET_200_sort_by_created(api_client_cognito):
    """List sources sorted by created (most recent first by default)"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"sort_by": "created"},
    )
    # Assert
    assert_json_response(response, 200)
    assert_headers_present(response, "x-paging-reverse-order")
    created = [source["created"] for source in response.json()]
    assert created == sorted(created, reverse=True)


def test_List_Sources_GET_400_sort_by(api_client_cognito):
    """List sources with an invalid sort_by value returns 400"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"sort_by": "invalid"},
    )
    # Assert
    assert_json_response(response, 400)


def test_List_Sources_GET_200_tag_name(api_client_cognito, stub_multi_source):
    """List sources with tag.{name} query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"tag.test": "this"},
    )
    # Assert
    assert_json_response(response, 200)
    response_json = remove_dynamic_props(response.json())
    assert 1 == len(response_json)
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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 5 == len(response_json)


def test_List_Sources_GET_400(api_client_cognito):
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"format": "bad"},
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


def test_List_Sources_GET_400_collected_by_ids(api_client_cognito, stub_multi_source):
    """List sources with collected_by_ids query specified"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"collected_by_ids": stub_multi_source["id"], "format": "bad"},
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


def test_List_Sources_GET_400_collected_by_ids_malformed(api_client_cognito):
    """List sources with a collected_by_ids value that is not a UUID list"""
    # Arrange
    path = "/sources"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"collected_by_ids": "not-a-uuid"},
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


def test_Source_Details_HEAD_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f"/sources/{stub_multi_source['id']}"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_Source_Details_HEAD_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 404, empty_body=True)


def test_Source_Details_GET_200(api_client_cognito, stub_data_source):
    # Arrange
    path = f"/sources/{stub_data_source['id']}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 200)
    response_json = remove_dynamic_props(response.json())
    assert_equal_unordered(stub_data_source, response_json)


def test_Source_Details_GET_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The requested Source does not exist." == response_json["message"]


def test_List_Source_Tags_HEAD_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f"/sources/{stub_multi_source['id']}/tags"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Source_Tags_HEAD_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/tags"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 404, empty_body=True)


def test_List_Source_Tags_GET_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f"/sources/{stub_multi_source['id']}/tags"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert stub_multi_source["tags"] == response_json


def test_List_Source_Tags_GET_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/tags"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The requested Source does not exist." == response_json["message"]


def test_Source_Tag_Value_HEAD_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f"/sources/{stub_multi_source['id']}/tags/flow_status"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_Source_Tag_Value_HEAD_404_bad_tag(api_client_cognito, stub_multi_source):
    # Arrange
    path = f"/sources/{stub_multi_source['id']}/tags/does_not_exist"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 404, empty_body=True)


def test_Source_Tag_Value_HEAD_404_bad_source_id(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/tags/flow_status"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 404, empty_body=True)


def test_Source_Tag_Value_GET_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f"/sources/{stub_multi_source['id']}/tags/flow_status"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert stub_multi_source["tags"]["flow_status"] == response_json


def test_Source_Tag_Value_GET_404_bad_tag(api_client_cognito, stub_multi_source):
    # Arrange
    path = f"/sources/{stub_multi_source['id']}/tags/does_not_exist"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The requested Source or tag does not exist." == response_json["message"]


def test_Source_Tag_Value_GET_404_bad_source_id(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/tags/flow_status"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The requested Source or tag does not exist." == response_json["message"]


def test_Create_or_Update_Source_Tag_PUT_204_create(
    api_client_cognito, stub_multi_source, expect_webhooks
):
    # Arrange
    tag_name = "pytest"
    tag_value = "test"
    path = f"/sources/{stub_multi_source['id']}/tags/{tag_name}"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=tag_value,
    )
    # Assert
    assert_json_response(response, 204, empty_body=True)
    stub_multi_source["tags"][tag_name] = tag_value
    expect_webhooks(
        {
            "event_type": "sources/updated",
            "event": {"source": stub_multi_source},
        },
    )


def test_Create_or_Update_Source_Tag_PUT_204_update(
    api_client_cognito, stub_multi_source, expect_webhooks
):
    # Arrange
    tag_name = "test"
    tag_value = "something else"
    path = f"/sources/{stub_multi_source['id']}/tags/{tag_name}"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=tag_value,
    )
    # Assert
    assert_json_response(response, 204, empty_body=True)
    stub_multi_source["tags"][tag_name] = tag_value
    expect_webhooks(
        {
            "event_type": "sources/updated",
            "event": {"source": stub_multi_source},
        },
    )


def test_Create_or_Update_Source_Tag_PUT_400(api_client_cognito, stub_multi_source):
    # Arrange
    path = f"/sources/{stub_multi_source['id']}/tags/pytest"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        data="test",
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


def test_Create_or_Update_Source_Tag_PUT_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/tags/pytest"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert (
        "The requested Source does not exist, or the tag name in the path is invalid."
        == response_json["message"]
    )


def test_Delete_Source_Tag_DELETE_204(
    api_client_cognito, stub_multi_source, expect_webhooks
):
    # Arrange
    tag_name = "pytest"
    path = f"/sources/{stub_multi_source['id']}/tags/{tag_name}"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    # Assert
    assert_json_response(response, 204, empty_body=True)
    del stub_multi_source["tags"][tag_name]
    expect_webhooks(
        {
            "event_type": "sources/updated",
            "event": {"source": stub_multi_source},
        },
    )


def test_Delete_Source_Tag_DELETE_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/tags/test"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert (
        "The requested Source ID or tag in the path is invalid."
        == response_json["message"]
    )


def test_Source_Description_HEAD_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f"/sources/{stub_multi_source['id']}/description"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_Source_Description_HEAD_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/description"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 404, empty_body=True)


def test_Source_Description_GET_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f"/sources/{stub_multi_source['id']}/description"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert stub_multi_source["description"] == response_json


def test_Source_Description_GET_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/description"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The requested Source does not exist." == response_json["message"]


def test_Create_or_Update_Source_Description_PUT_204_create(
    api_client_cognito, stub_audio_source, expect_webhooks
):
    # Arrange
    value = "pytest - audio"
    path = f"/sources/{stub_audio_source['id']}/description"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=value,
    )
    # Assert
    assert_json_response(response, 204, empty_body=True)
    stub_audio_source["description"] = value
    expect_webhooks(
        {
            "event_type": "sources/updated",
            "event": {"source": stub_audio_source},
        },
    )


def test_Create_or_Update_Source_Description_PUT_204_update(
    api_client_cognito, stub_video_source, expect_webhooks
):
    # Arrange
    value = "pytest"
    path = f"/sources/{stub_video_source['id']}/description"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=value,
    )
    # Assert
    assert_json_response(response, 204, empty_body=True)
    stub_video_source["description"] = value
    expect_webhooks(
        {
            "event_type": "sources/updated",
            "event": {"source": stub_video_source},
        },
    )


def test_Create_or_Update_Source_Description_PUT_400(
    api_client_cognito, stub_video_source
):
    # Arrange
    path = f"/sources/{stub_video_source['id']}/description"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        data="test",
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


def test_Create_or_Update_Source_Description_PUT_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/description"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The requested Source does not exist." == response_json["message"]


def test_Delete_Source_Description_DELETE_204(
    api_client_cognito, stub_audio_source, expect_webhooks
):
    # Arrange
    path = f"/sources/{stub_audio_source['id']}/description"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    # Assert
    assert_json_response(response, 204, empty_body=True)
    del stub_audio_source["description"]
    expect_webhooks(
        {
            "event_type": "sources/updated",
            "event": {"source": stub_audio_source},
        },
    )


def test_Delete_Source_Description_DELETE_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/description"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The Source ID in the path is invalid." == response_json["message"]


def test_Source_Label_HEAD_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f"/sources/{stub_multi_source['id']}/label"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_Source_Label_HEAD_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/label"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 404, empty_body=True)


def test_Source_Label_GET_200(api_client_cognito, stub_multi_source):
    # Arrange
    path = f"/sources/{stub_multi_source['id']}/label"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert stub_multi_source["label"] == response_json


def test_Source_Label_GET_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/label"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The requested Source does not exist." == response_json["message"]


def test_Create_or_Update_Source_Label_PUT_204_create(
    api_client_cognito, stub_audio_source, expect_webhooks
):
    # Arrange
    value = "pytest - audio"
    path = f"/sources/{stub_audio_source['id']}/label"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=value,
    )
    # Assert
    assert_json_response(response, 204, empty_body=True)
    stub_audio_source["label"] = value
    expect_webhooks(
        {
            "event_type": "sources/updated",
            "event": {"source": stub_audio_source},
        },
    )


def test_Create_or_Update_Source_Label_PUT_204_update(
    api_client_cognito, stub_video_source, expect_webhooks
):
    # Arrange
    value = "pytest"
    path = f"/sources/{stub_video_source['id']}/label"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=value,
    )
    # Assert
    assert_json_response(response, 204, empty_body=True)
    stub_video_source["label"] = value
    expect_webhooks(
        {
            "event_type": "sources/updated",
            "event": {"source": stub_video_source},
        },
    )


def test_Create_or_Update_Source_Label_PUT_400(api_client_cognito, stub_video_source):
    # Arrange
    path = f"/sources/{stub_video_source['id']}/label"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        data="test",
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


def test_Create_or_Update_Source_Label_PUT_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/label"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The requested Source does not exist." == response_json["message"]


def test_Delete_Source_Label_DELETE_204(
    api_client_cognito, stub_audio_source, expect_webhooks
):
    # Arrange
    path = f"/sources/{stub_audio_source['id']}/label"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    # Assert
    assert_json_response(response, 204, empty_body=True)
    del stub_audio_source["label"]
    expect_webhooks(
        {
            "event_type": "sources/updated",
            "event": {"source": stub_audio_source},
        },
    )


def test_Delete_Source_Label_DELETE_404(api_client_cognito):
    # Arrange
    path = f"/sources/{ID_404}/label"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The requested Source ID in the path is invalid." == response_json["message"]
